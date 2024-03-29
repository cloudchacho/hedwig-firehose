package firehose

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
)

const DefaultVisibilityTimeoutS = time.Second * 20

type ProcessSettings struct {
	// interval when leader moves files to final bucket
	ScrapeInterval int

	// interval when follower flushes to staging bucket
	FlushAfter int

	// bucket where leader file is saved
	MetadataBucket string

	// bucket where follower put intermediate files to be moved by leader
	StagingBucket string

	// final bucket for firehose files
	OutputBucket string

	// timeout before determining if node is a leader panics
	AcquireRoleTimeout int
}

// MsgToFilePrefix outputs the fileprefix (should be one of fp.fileprefixes) in StagingBucket and OutputBucket for a given hedwig message
type MsgToFilePrefix func(message *hedwig.Message) (string, error)

type ReceivedMessage struct {
	message *hedwig.Message
	errCh   chan error
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

type byTimestamp []*hedwig.Message

func (t byTimestamp) Len() int {
	return len(t)
}
func (t byTimestamp) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t byTimestamp) Less(i, j int) bool {
	return t[i].Metadata.Timestamp.Unix() < t[j].Metadata.Timestamp.Unix()
}

type leaderFile struct {
	Timestamp    string
	DeploymentId string
	NodeId       string
}

type LeaderFileExists struct{}

func (e LeaderFileExists) Error() string {
	return "Leader file already exists"
}

type indexFile struct {
	Name         string `json:"name"`
	MinTimestamp int64  `json:"min_timestamp"`
	MaxTimestamp int64  `json:"max_timestamp"`
}

// StorageBackend is used for interacting with storage
type StorageBackend interface {
	// CreateWriter returns a writer for specified uploadlocation
	CreateWriter(ctx context.Context, uploadBucket string, uploadLocation string) (io.WriteCloser, error)

	// CreateReader returns a reader for specified uploadlocation
	CreateReader(ctx context.Context, uploadBucket string, uploadLocation string) (io.ReadCloser, error)

	// ListFilesPrefix should list all objects with a certain prefix
	ListFilesPrefix(ctx context.Context, bucket string, prefix string) ([]string, error)

	// DeleteFile deletes the object at the specified location
	DeleteFile(ctx context.Context, bucket string, location string) error

	// GetNodeId returns the id of the node/machine running the firehose process
	GetNodeId(ctx context.Context) string

	// GetDeploymentId returns the id of the deployment version of firehose currently running
	GetDeploymentId(ctx context.Context) string

	// WriteLeaderFile should return LeaderFileExists error if the leader file already exists fileContents should be json string of leaderFile
	WriteLeaderFile(ctx context.Context, metadataBucket string, fileContents []byte) error
}

type Clock struct {
	// If non-nil this is a synthetic time for testing
	Instant time.Time
	// A lock protecting the synthetic time, so that one thread can update the clock
	// while firehose is running. It's only used when Instant is non-nil, so it's safe
	// to update a non-nil Instant to another non-nil Instant but it's a race to
	// update a nil Instant to a non-nil Instant while firehose is running.
	mu sync.Mutex
}

func (this *Clock) Now() time.Time {
	if this == nil {
		return time.Now()
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.Instant
}

func (this *Clock) Change(time time.Time) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.Instant = time
}

type Firehose struct {
	processSettings ProcessSettings
	StorageBackend  StorageBackend
	hedwigConsumer  *hedwig.QueueConsumer
	filePrefixes    []string
	msgToFilePrefix MsgToFilePrefix
	logger          hedwig.Logger
	registry        hedwig.CallbackRegistry
	HedwigFirehose  *hedwig.Firehose
	flushLock       sync.Mutex
	flushCh         chan error
	messageCh       chan ReceivedMessage
	listenRequest   hedwig.ListenRequest
	Clock           *Clock
}

func (fp *Firehose) flushCron(ctx context.Context) {
	errChannelMapping := make(map[hedwig.MessageTypeMajorVersion][]chan error)
	writerMapping := make(map[hedwig.MessageTypeMajorVersion]io.WriteCloser)
	currentTime := fp.Clock.Now()
	timerCh := time.After(time.Duration(fp.processSettings.FlushAfter) * time.Second)
	// go through all msgs and write to msgtype folder
	for {
		select {
		case <-timerCh:
			// close all writers and associated errChannels
			for key, writer := range writerMapping {
				err := writer.Close()
				errChannels := errChannelMapping[key]
				if err != nil {
					for _, errCh := range errChannels {
						errCh <- err
					}
				} else {
					for _, errCh := range errChannels {
						close(errCh)
					}
				}
			}
			// start a new flushcron timer and reset writerMapping/errChannelMapping, as this one is done
			writerMapping = make(map[hedwig.MessageTypeMajorVersion]io.WriteCloser)
			errChannelMapping = make(map[hedwig.MessageTypeMajorVersion][]chan error)
			currentTime = fp.Clock.Now()
			timerCh = time.After(time.Duration(fp.processSettings.FlushAfter) * time.Second)
		case messageAndChan := <-fp.messageCh:
			message := messageAndChan.message
			errCh := messageAndChan.errCh
			key := hedwig.MessageTypeMajorVersion{
				MessageType:  message.Type,
				MajorVersion: uint(message.DataSchemaVersion.Major()),
			}
			// if writer doesn't exist create in mapping
			nodeId := fp.StorageBackend.GetNodeId(ctx)
			if _, ok := writerMapping[key]; !ok {
				subName, err := fp.msgToFilePrefix(message)
				if !contains(fp.filePrefixes, subName) {
					panic(fmt.Sprintf("output of msgToFilePrefix %s not in filePrefixes", subName))
				}
				if err != nil {
					errCh <- err
					continue
				}
				uploadLocation := fmt.Sprintf("%s/%s/%s/%s", subName, nodeId, currentTime.Format("2006/1/2"), fmt.Sprint(currentTime.Unix()))
				writer, err := fp.StorageBackend.CreateWriter(ctx, fp.processSettings.StagingBucket, uploadLocation)
				if err != nil {
					errCh <- err
					continue
				}
				writerMapping[key] = writer
			}
			msgTypeWriter := writerMapping[key]
			payload, err := fp.HedwigFirehose.Serialize(message)
			if err != nil {
				errCh <- err
				continue
			}
			_, err = msgTypeWriter.Write(payload)
			if err != nil {
				errCh <- err
				continue
			}
			errChannelMapping[key] = append(errChannelMapping[key], errCh)
		case <-ctx.Done():
			// if ctx closes error all in flight messages
			for key := range writerMapping {
				errChannels := errChannelMapping[key]
				err := ctx.Err()
				for _, errCh := range errChannels {
					errCh <- err
				}
			}
			return
		}
	}

}

func (fp *Firehose) RunFollower(ctx context.Context) error {
	// run an infinite loop until canceled
	// and call handleMessage
	go fp.flushCron(ctx)
	err := fp.hedwigConsumer.ListenForMessages(ctx, fp.listenRequest)
	// consumer errored so panic
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		panic(err)
	}
	return err
}

func (fp *Firehose) handleMessage(ctx context.Context, message *hedwig.Message) error {
	ch := make(chan error)
	fp.messageCh <- ReceivedMessage{
		message: message,
		errCh:   ch,
	}
	// wait until message flushed into GCS file.
	err := <-ch

	if err != nil {
		fp.logger.Error(ctx, err, "handleMessage Failed",
			"messageType", message.Type,
			"errString", err.Error(),
		)
	} else {
		fp.logger.Debug(ctx, "Message Handled",
			"MessageType", message.Type,
			"MessageId", message.ID,
		)
	}
	return err
}

func (fp *Firehose) moveFilesToOutputBucket(ctx context.Context, filePathPrefix string, currTime time.Time) error {
	// read from staging
	fileNames, err := fp.StorageBackend.ListFilesPrefix(ctx, fp.processSettings.StagingBucket, filePathPrefix)
	if err != nil {
		return err
	}
	var msgs byTimestamp
	var minTimestamp int64 = math.MaxInt64
	var maxTimestamp int64 = math.MinInt64
	for _, fileName := range fileNames {
		r, err := fp.StorageBackend.CreateReader(ctx, fp.processSettings.StagingBucket, fileName)
		defer r.Close()
		if err != nil {
			return err
		}
		res, err := fp.HedwigFirehose.Deserialize(r)
		if err != nil {
			return err
		}
		for _, r := range res {
			msgs = append(msgs, &r)
			t := r.Metadata.Timestamp.Unix()
			if t < minTimestamp {
				minTimestamp = t
			}
			if t > maxTimestamp {
				maxTimestamp = t
			}
		}
	}
	if len(msgs) > 0 {
		// sort by timestamp
		sort.Sort(msgs)
		filename := fmt.Sprint(currTime.Unix())
		uploadLocation := fmt.Sprintf("%s/%s/%s", filePathPrefix, currTime.Format("2006/1/2"), filename)
		r, err := fp.StorageBackend.CreateWriter(ctx, fp.processSettings.OutputBucket, uploadLocation)
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			payload, err := fp.HedwigFirehose.Serialize(msg)
			if err != nil {
				return err
			}
			_, err = r.Write(payload)
			if err != nil {
				return err
			}
		}
		err = r.Close()
		if err != nil {
			return err
		}
		// delete files when written to final output path
		for _, fileName := range fileNames {
			// ignore errors when deleting, picked up again on next run
			_ = fp.StorageBackend.DeleteFile(ctx, fp.processSettings.StagingBucket, fileName)
		}

		err = fp.writeIndex(ctx, filePathPrefix, currTime, filename, minTimestamp, maxTimestamp)
		if err != nil {
			return err
		}
	}
	return nil
}

// maintain an index file (one for every output directory) listing all the data files and their timestamp range
func (fp *Firehose) writeIndex(ctx context.Context, filePathPrefix string, currTime time.Time, name string, minTimestamp int64, maxTimestamp int64) error {
	indexEntry, err := json.Marshal(indexFile{
		Name:         name,
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
	})
	indexEntry = append(indexEntry, '\n')
	if err != nil {
		return err
	}
	indexLocation := fmt.Sprintf("%s/%s/_metadata.ndjson", filePathPrefix, currTime.Format("2006/1/2"))
	// GCS objects are immutable: we cannot append to the index. So we read the old index,
	// append a line, and write a new one. At 5 minutes per entry and a new index file per day,
	// there are a max of 288 entries per index file, so it isn't too big.
	index, err := fp.StorageBackend.CreateReader(ctx, fp.processSettings.OutputBucket, indexLocation)
	var indexContents []byte
	if err == nil {
		defer index.Close()
		indexContents, err = io.ReadAll(index)
		if err != nil {
			return err
		}
	} else if err != storage.ErrObjectNotExist {
		return err
	}

	indexWriter, err := fp.StorageBackend.CreateWriter(ctx, fp.processSettings.OutputBucket, indexLocation)
	if err != nil {
		return err
	}
	_, err = indexWriter.Write(indexContents)
	if err != nil {
		return err
	}
	_, err = indexWriter.Write(indexEntry)
	if err != nil {
		return err
	}
	err = indexWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

func (fp *Firehose) RunLeader(ctx context.Context) error {
	currentTime := fp.Clock.Now()
	timerCh := time.After(time.Duration(fp.processSettings.ScrapeInterval) * time.Second)
	// go through all msgs and write to msgtype folder

	for {
		select {
		case <-timerCh:
			wg := &sync.WaitGroup{}
			for _, filePrefix := range fp.filePrefixes {
				wg.Add(1)
				go func(filePrefix string) {
					defer wg.Done()
					err := fp.moveFilesToOutputBucket(ctx, filePrefix, currentTime)
					// just logs errors but will retry on next run of leader
					if err != nil {
						fp.logger.Error(ctx, err, "moving files failed",
							"filePrefix", filePrefix,
						)
					}
				}(filePrefix)
			}
			wg.Wait()
			// restart scrape interval and run leader again
			currentTime = fp.Clock.Now()
			timerCh = time.After(time.Duration(fp.processSettings.ScrapeInterval) * time.Second)
		case <-ctx.Done():
			// delete leader file on shutdown, on failure will have to manually delete
			// New context to delete without context being canceled
			background := context.Background()
			err := fp.StorageBackend.DeleteFile(background, fp.processSettings.MetadataBucket, "leader.json")
			if err != nil {
				fp.logger.Error(ctx, err, "deleting leader file on shutdown failed")
			}
			err = ctx.Err()
			// dont return err if context stopped process
			if err != context.Canceled && err != context.DeadlineExceeded {
				fp.logger.Error(ctx, err, "RunLeader failed",
					"currentTime", currentTime.Unix(),
				)
			}
			return err
		}
	}
}

func (fp *Firehose) IsLeader(ctx context.Context) (bool, error) {
	nodeId := fp.StorageBackend.GetNodeId(ctx)
	deploymentId := fp.StorageBackend.GetDeploymentId(ctx)
	if nodeId == "" || deploymentId == "" {
		panic("nodeId or deploymentId can not be determined")
	}
	jsonStr, err := json.Marshal(leaderFile{
		Timestamp:    fmt.Sprint(fp.Clock.Now().Unix()),
		DeploymentId: deploymentId,
		NodeId:       nodeId,
	})
	if err != nil {
		return false, err
	}
	leaderWriteErr := fp.StorageBackend.WriteLeaderFile(ctx, fp.processSettings.MetadataBucket, jsonStr)

	if leaderWriteErr == nil {
		return true, nil
	}
	_, ok := leaderWriteErr.(LeaderFileExists)
	if ok {
		r, err := fp.StorageBackend.CreateReader(ctx, fp.processSettings.MetadataBucket, "leader.json")
		if err != nil {
			return false, err
		}
		data, err := ioutil.ReadAll(r)
		if err != nil {
			return false, err
		}
		var result leaderFile
		err = json.Unmarshal(data, &result)
		if err != nil {
			return false, err
		}

		if result.DeploymentId != deploymentId {
			return false, fmt.Errorf("deploymentId does not match current leader")
		}
		if result.NodeId == nodeId && result.DeploymentId == deploymentId {
			return true, nil
		}

		return false, nil
	} else {
		return false, leaderWriteErr
	}
}

// RunFirehose starts a Firehose running in leader or follower mode
func (fp *Firehose) RunFirehose(ctx context.Context) error {
	// 1. on start up determine if leader or followerBackend
	globalTimeout := fp.processSettings.AcquireRoleTimeout
	timeCheckingLeader := 0
	leader := false
outer:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// poll for valid isLeader every 2 seconds
			interval := 2
			<-time.After(time.Second * time.Duration(interval))
			var err error
			leader, err = fp.IsLeader(ctx)
			if err != nil {
				timeCheckingLeader = timeCheckingLeader + interval
				if timeCheckingLeader >= globalTimeout {
					return fmt.Errorf("failed to read leader file or deploymentId did not match")
				}
				// retry if error reading leader file
				continue
			}
			break outer
		}
	}
	nodeId := fp.StorageBackend.GetNodeId(ctx)
	if leader {
		// 2. if leader call RunLeader
		fp.logger.Debug(ctx, "Starting Leader",
			"NodeId", nodeId,
		)
		return fp.RunLeader(ctx)
	} else {
		// 3. else follower call RunFollower
		fp.logger.Debug(ctx, "Starting Follower",
			"NodeId", nodeId,
		)
		return fp.RunFollower(ctx)
	}
}

func NewFirehose(consumerBackend hedwig.ConsumerBackend, encoderDecoder hedwig.EncoderDecoder, msgList []hedwig.MessageTypeMajorVersion, filePrefixes []string, mtfp MsgToFilePrefix, storageBackend StorageBackend, listenRequest hedwig.ListenRequest, consumerSettings gcp.Settings, processSettings ProcessSettings, logger hedwig.Logger) (*Firehose, error) {
	registry := hedwig.CallbackRegistry{}

	hedwigFirehose := hedwig.NewFirehose(encoderDecoder, encoderDecoder)
	f := &Firehose{
		processSettings: processSettings,
		StorageBackend:  storageBackend,
		HedwigFirehose:  hedwigFirehose,
		logger:          logger,
		messageCh:       make(chan ReceivedMessage),
		listenRequest:   listenRequest,
	}
	for _, msgTypeVer := range msgList {
		registry[msgTypeVer] = f.handleMessage
	}
	hedwigConsumer := hedwig.NewQueueConsumer(consumerBackend, encoderDecoder, logger, registry)
	f.hedwigConsumer = hedwigConsumer
	f.registry = registry
	f.filePrefixes = filePrefixes
	f.msgToFilePrefix = mtfp
	return f, nil
}

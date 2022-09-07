package firehose

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"
	"time"

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
	Instant time.Time
}

func (this *Clock) Now() time.Time {
	if this == nil {
		return time.Now()
	}
	return this.Instant
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
		}
	}
	if len(msgs) > 0 {
		// sort by timestamp
		sort.Sort(msgs)
		uploadLocation := fmt.Sprintf("%s/%s/%s.gz", filePathPrefix, currTime.Format("2006/1/2"), fmt.Sprint(currTime.Unix()))
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
	if leader {
		// 2. if leader call RunLeader
		return fp.RunLeader(ctx)
	} else {
		// 3. else follower call RunFollower
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

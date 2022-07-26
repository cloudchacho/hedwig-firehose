package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
)

const defaultVisibilityTimeoutS = time.Second * 20

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
}

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

// StorageBackend is used for interacting with storage
type StorageBackend interface {
	CreateWriter(ctx context.Context, uploadBucket string, uploadLocation string) (io.WriteCloser, error)
	CreateReader(ctx context.Context, uploadBucket string, uploadLocation string) (io.ReadCloser, error)
	// ListFilesPrefix should list all objects with a certain prefix
	ListFilesPrefix(ctx context.Context, bucket string, prefix string) ([]string, error)
	DeleteFile(ctx context.Context, bucket string, location string) error
}

type Clock struct {
	instant time.Time
}

func (this *Clock) Now() time.Time {
	if this == nil {
		return time.Now()
	}
	return this.instant
}

type Firehose struct {
	processSettings ProcessSettings
	storageBackend  StorageBackend
	hedwigConsumer  *hedwig.QueueConsumer
	logger          hedwig.Logger
	registry        hedwig.CallbackRegistry
	hedwigFirehose  *hedwig.Firehose
	flushLock       sync.Mutex
	flushCh         chan error
	messageCh       chan ReceivedMessage
	listenRequest   hedwig.ListenRequest
	clock           *Clock
}

func (fp *Firehose) flushCron(ctx context.Context) {
	errChannelMapping := make(map[hedwig.MessageTypeMajorVersion][]chan error)
	writerMapping := make(map[hedwig.MessageTypeMajorVersion]io.WriteCloser)
	currentTime := fp.clock.Now()
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
			timerCh = time.After(time.Duration(fp.processSettings.FlushAfter) * time.Second)
		case messageAndChan := <-fp.messageCh:
			message := messageAndChan.message
			errCh := messageAndChan.errCh
			key := hedwig.MessageTypeMajorVersion{
				MessageType:  message.Type,
				MajorVersion: uint(message.DataSchemaVersion.Major()),
			}
			// if writer doesn't exist create in mapping
			if _, ok := writerMapping[key]; !ok {
				// TODO: use node id in this path
				uploadLocation := fmt.Sprintf("%s/%s/%s/%s", key.MessageType, fmt.Sprint(key.MajorVersion), currentTime.Format("2006/1/2"), fmt.Sprint(currentTime.Unix()))
				writer, err := fp.storageBackend.CreateWriter(ctx, fp.processSettings.StagingBucket, uploadLocation)
				if err != nil {
					errCh <- err
					continue
				}
				writerMapping[key] = writer
			}
			msgTypeWriter := writerMapping[key]
			payload, err := fp.hedwigFirehose.Serialize(message)
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
			for key, _ := range writerMapping {
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
	return err
}

func (fp *Firehose) moveFilesToOutputBucket(ctx context.Context, mtmv hedwig.MessageTypeMajorVersion, currTime time.Time) error {
	// read from staging
	filePathPrefix := fmt.Sprintf("%s/%s", mtmv.MessageType, fmt.Sprint(mtmv.MajorVersion))
	fileNames, err := fp.storageBackend.ListFilesPrefix(ctx, fp.processSettings.StagingBucket, filePathPrefix)
	if err != nil {
		return err
	}
	var msgs byTimestamp
	for _, fileName := range fileNames {
		r, err := fp.storageBackend.CreateReader(ctx, fp.processSettings.StagingBucket, fileName)
		defer r.Close()
		if err != nil {
			return err
		}
		res, err := fp.hedwigFirehose.Deserialize(r)
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
		uploadLocation := fmt.Sprintf("%s/%s/%s/%s-%s-%s.gz", mtmv.MessageType, fmt.Sprint(mtmv.MajorVersion), currTime.Format("2006/1/2"), mtmv.MessageType, fmt.Sprint(mtmv.MajorVersion), fmt.Sprint(currTime.Unix()))
		r, err := fp.storageBackend.CreateWriter(ctx, fp.processSettings.OutputBucket, uploadLocation)
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			payload, err := fp.hedwigFirehose.Serialize(msg)
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
			_ = fp.storageBackend.DeleteFile(ctx, fp.processSettings.StagingBucket, fileName)
		}
	}
	return nil
}

func (fp *Firehose) RunLeader(ctx context.Context) {
	currentTime := fp.clock.Now()
	timerCh := time.After(time.Duration(fp.processSettings.ScrapeInterval) * time.Second)
	// go through all msgs and write to msgtype folder
	for {
		select {
		case <-timerCh:
			wg := &sync.WaitGroup{}
			for mtmv, _ := range fp.registry {
				wg.Add(1)
				go func(mtmv hedwig.MessageTypeMajorVersion) {
					defer wg.Done()
					err := fp.moveFilesToOutputBucket(ctx, mtmv, currentTime)
					// just logs errors but will retry on next run of leader
					if err != nil {
						fmt.Println(err.Error())
						fp.logger.Error(ctx, err, "moving files failed",
							"messageType", mtmv.MessageType,
							"messageVersion", fmt.Sprint(mtmv.MajorVersion),
						)
					}
				}(mtmv)
			}
			wg.Wait()
			// restart scrape interval and run leader again
			timerCh = time.After(time.Duration(fp.processSettings.ScrapeInterval) * time.Second)
		case <-ctx.Done():
			err := ctx.Err()
			// dont return err if context stopped process
			if err != context.Canceled && err != context.DeadlineExceeded {
				fp.logger.Error(ctx, err, "RunLeader failed",
					"currentTime", currentTime.Unix(),
				)
			}
			return
		}
	}
}

// RunFirehose starts a Firehose running in leader of follower mode
func (f *Firehose) RunFirehose() {
	// 1. on start up determine if leader or followerBackend
	// 2. if leader call RunLeader
	// 3. else follower call RunFollower
}

func NewFirehose(consumerBackend hedwig.ConsumerBackend, encoderDecoder hedwig.EncoderDecoder, msgList []hedwig.MessageTypeMajorVersion, storageBackend StorageBackend, listenRequest hedwig.ListenRequest, consumerSettings gcp.Settings, processSettings ProcessSettings, logger hedwig.Logger) (*Firehose, error) {
	registry := hedwig.CallbackRegistry{}

	hedwigFirehose := hedwig.NewFirehose(encoderDecoder, encoderDecoder)
	f := &Firehose{
		processSettings: processSettings,
		storageBackend:  storageBackend,
		hedwigFirehose:  hedwigFirehose,
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
	return f, nil
}

func main() {
	fmt.Println("Hello World")
}

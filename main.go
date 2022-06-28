package main

import (
	"context"
	"fmt"
	"io"
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

// ReceivedMessage is the message as received by a transport backend.
type ReceivedMessage struct {
	Payload          []byte
	Attributes       map[string]string
	ProviderMetadata interface{}
}

// StorageBackendCreator is used for read/write to storage
type StorageBackendCreator interface {
	CreateWriter(ctx context.Context, uploadBucket string, uploadLocation string) (io.WriteCloser, error)
	CreateReader(ctx context.Context, uploadBucket string, uploadLocation string) (io.ReadCloser, error)
}

type Firehose struct {
	processSettings       ProcessSettings
	storageBackendCreator StorageBackendCreator
	hedwigConsumer        *hedwig.QueueConsumer
	hedwigFirehose        *hedwig.Firehose
	flushLock             sync.Mutex
	flushCh               chan error
	messageCh             chan struct {
		message *hedwig.Message
		errCh   chan error
	}
}

func (fp *Firehose) flushCron(ctx context.Context) {
	errChannelMapping := make(map[hedwig.MessageTypeMajorVersion][]chan error)
	writerMapping := make(map[hedwig.MessageTypeMajorVersion]io.WriteCloser)
	currentTime := time.Now()
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
			// start a new flushcron channel, as this one is done
			go fp.flushCron(ctx)
			return
		case messageAndChan := <-fp.messageCh:
			message := messageAndChan.message
			errCh := messageAndChan.errCh
			key := hedwig.MessageTypeMajorVersion{
				MessageType:  message.Type,
				MajorVersion: uint(message.DataSchemaVersion.Major()),
			}
			// if writer doesn't exist create in mapping
			if _, ok := writerMapping[key]; !ok {
				uploadLocation := fmt.Sprintf("%s/%s/%s/%s", key.MessageType, fmt.Sprint(key.MajorVersion), currentTime.Format("2006/1/2"), fmt.Sprint(currentTime.Unix()))
				writer, err := fp.storageBackendCreator.CreateWriter(ctx, fp.processSettings.StagingBucket, uploadLocation)
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
		}
	}

}

func (fp *Firehose) RunFollower(ctx context.Context) {
	// run an infinite loop until canceled
	// and call handleMessage
	go fp.flushCron(ctx)
	// should this be configureable?
	lr := hedwig.ListenRequest{1, defaultVisibilityTimeoutS, 1}
	fp.hedwigConsumer.ListenForMessages(ctx, lr)
}

func (fp *Firehose) handleMessage(ctx context.Context, message *hedwig.Message) error {
	ch := make(chan error)
	fp.messageCh <- struct {
		message *hedwig.Message
		errCh   chan error
	}{message, ch}
	// wait until message flushed into GCS file.
	err := <-ch
	return err
}

func (fp *Firehose) RunLeader() {
}

// RunFirehose starts a Firehose running in leader of follower mode
func (f *Firehose) RunFirehose() {
	// 1. on start up determine if leader or followerBackend
	// 2. if leader call RunLeader
	// 3. else follower call RunFollower
}

func NewFirehose(consumerBackend hedwig.ConsumerBackend, encoderDecoder hedwig.EncoderDecoder, msgList []hedwig.MessageTypeMajorVersion, storageBackendCreator StorageBackendCreator, consumerSettings gcp.Settings, processSettings ProcessSettings, logger hedwig.Logger) (*Firehose, error) {
	registry := hedwig.CallbackRegistry{}

	hedwigFirehose := hedwig.NewFirehose(encoderDecoder, encoderDecoder)
	f := &Firehose{
		processSettings:       processSettings,
		storageBackendCreator: storageBackendCreator,
		hedwigFirehose:        hedwigFirehose,
		messageCh: make(chan struct {
			message *hedwig.Message
			errCh   chan error
		}),
	}
	for _, msgTypeVer := range msgList {
		registry[msgTypeVer] = f.handleMessage
	}
	hedwigConsumer := hedwig.NewQueueConsumer(consumerBackend, encoderDecoder, logger, registry)
	f.hedwigConsumer = hedwigConsumer
	return f, nil
}

func main() {
	fmt.Println("Hello World")
}

package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"google.golang.org/protobuf/proto"
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

//// StorageBackend is used for read/write to storage
//type StorageBackend interface {
//	UploadFile(ctx context.Context, data []byte, uploadBucket string, uploadLocation string) error
//
//	ReadFile(ctx context.Context, readBucket string, readLocation string) ([]byte, error)
//}

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

func (fp *Firehose) flushCron() {
	timerCh := time.After(time.Duration(fp.processSettings.FlushAfter) * time.Second)
	errChannelMapping := make(map[string][]chan error)
	writerMapping := make(map[string]io.WriteCloser)
	currentTime := time.Now()
	// go through all msgs and write to msgtype folder
outer:
	for {
		select {
		case <-timerCh:
			break outer
		case messageAndChan := <-fp.messageCh:
			message := messageAndChan.message
			errCh := messageAndChan.errCh
			msgType := message.Type
			// if writer doesn't exist create in mapping
			if _, ok := writerMapping["foo"]; !ok {
				uploadLocation := fmt.Sprintf("%s/%s/%s", msgType, currentTime.Format("2006/1/2"), currentTime.Unix())
				writer, err := fp.storageBackendCreator.CreateWriter(context.Background(), fp.processSettings.StagingBucket, uploadLocation)
				if err != nil {
					errCh <- err
					continue
				}
				writerMapping[msgType] = writer
			}
			msgTypeWriter := writerMapping[msgType]
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
			errChannels := errChannelMapping[msgType]
			errChannels = append(errChannels, errCh)
		}
	}

	// close all writers and associated errChannels
	for msgType, writer := range writerMapping {
		err := writer.Close()
		errChannels := errChannelMapping[msgType]
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
	go fp.flushCron()
}

func (fp *Firehose) RunFollower(ctx context.Context) {
	// run an infinite loop until canceled
	// and call handleMessage
	go fp.flushCron()
	// should this be configureable?
	lr := hedwig.ListenRequest{1, defaultVisibilityTimeoutS, 1}
	fp.hedwigConsumer.ListenForMessages(ctx, lr)
}

func (fp *Firehose) handleMessage(message *hedwig.Message) error {
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

func NewFirehose(consumerBackend hedwig.ConsumerBackend, encoder hedwig.Encoder, decoder hedwig.Decoder, storageBackendCreator StorageBackendCreator, consumerSettings gcp.Settings, processSettings ProcessSettings, logger hedwig.Logger) (*Firehose, error) {
	// TODO: register same callback with all messages to basically write to memory until flush
	registry := hedwig.CallbackRegistry{}

	hedwigConsumer := hedwig.NewQueueConsumer(consumerBackend, decoder, logger, registry)
	hedwigFirehose := hedwig.NewFirehose(encoder, decoder)
	f := &Firehose{
		processSettings:       processSettings,
		storageBackendCreator: storageBackendCreator,
		hedwigConsumer:        hedwigConsumer,
		hedwigFirehose:        hedwigFirehose,
	}
	return f, nil
}

func main() {
	fmt.Println("Hello World")
}

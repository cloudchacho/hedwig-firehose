package main

import (
	"context"
	"fmt"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"google.golang.org/protobuf/proto"
)

type ProcessSettings struct {
	// interval when leader moves files to final bucket
	ScrapeInterval int

	// interval when follower flushes to staging bucket
	FlushAfter int
}

// ReceivedMessage is the message as received by a transport backend.
type ReceivedMessage struct {
	Payload          []byte
	Attributes       map[string]string
	ProviderMetadata interface{}
}

// FirehoseBackend is used for read/write to storage
type FirehoseBackend interface {
	UploadFile(ctx context.Context, data []byte, uploadBucket string, uploadLocation string) error

	ReadFile(ctx context.Context, readBucket string, readLocation string) ([]byte, error)
}

type Firehose struct {
	processSettings ProcessSettings
	firehoseBackend FirehoseBackend
	hedwigConsumer *hedwig.QueueConsumer
}

func (fp *Firehose) WriteMessages() {
}

func (fp *Firehose) RunFollower() {
}

func (fp *Firehose) RunLeader() {
}

// RunFirehose starts a Firehose running in leader of follower mode
func (f *Firehose) RunFirehose() {
	// 1. on start up determine if leader or followerBackend
	// 2. if leader call RunFollower
	// 3. else follower call RunFollower
}

func NewFirehose(firehoseBackend FirehoseBackend, consumerSettings gcp.Settings, processSettings ProcessSettings) (*Firehose, error) {
	// If we want to inject config from env, pass in when creating new

	// TODO: add logger here
	getLoggerFunc := func(_ context.Context) hedwig.Logger {
		return nil
	}
	backend := gcp.NewBackend(consumerSettings, getLoggerFunc)
	// TODO: add from schema generation here
	encoder, err := protobuf.NewMessageEncoderDecoder([]proto.Message{})
	if err != nil {
		return nil, err
	}
	// TODO: register same callback with all message to basically write to memory until flush
	registry := hedwig.CallbackRegistry {}

	hedwigConsumer := hedwig.NewQueueConsumer(backend, encoder, getLoggerFunc, registry)
	f := &Firehose{
		processSettings: processSettings,
		firehoseBackend: firehoseBackend,
		hedwigConsumer:  hedwigConsumer,
	}
	return f, nil
}

func main() {
	fmt.Println("Hello World")
}

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Masterminds/semver"
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

// StorageBackend is used for read/write to storage
type StorageBackend interface {
	UploadFile(ctx context.Context, data []byte, uploadBucket string, uploadLocation string) error

	ReadFile(ctx context.Context, readBucket string, readLocation string) ([]byte, error)
}

type Firehose struct {
	processSettings ProcessSettings
	storageBackend  StorageBackend
	hedwigConsumer  *hedwig.QueueConsumer
	hedwigFirehose  *hedwig.Firehose
	messageBuffer   []*hedwig.Message
	flushLock       sync.Mutex
	flushCh         chan error
}

func (fp *Firehose) WriteMessage(wg *sync.WaitGroup, message *hedwig.Message) error {
	defer wg.Done()
	data := fp.hedwigFirehose.Serialize(message)
	currentTime := time.Now()
	uploadLocation := fmt.Sprintf("%s/%s/%s", message.Type, currentTime.Format("2006/1/2"), "filetime?")
	return fp.storageBackend.UploadFile(context.Background(), data, fp.processSettings.StagingBucket, uploadLocation)
}

func (fp *Firehose) flushCron() {
	<-time.After(time.Duration(fp.processSettings.FlushAfter) * time.Second)
	fp.flushLock.Lock()
	defer fp.flushLock.Unlock()
	var wg sync.WaitGroup
	for _, msg := range fp.messageBuffer {
		wg.Add(1)
		err := fp.WriteMessage(&wg, msg)
		if err != nil {
			fp.flushCh <- err
			break
		}
	}
	// close channel to unblock all handleMessage functions (should ack msg from queue)
	close(fp.flushCh)
	fp.flushCh = make(chan error)
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
	fp.messageBuffer = append(fp.messageBuffer, message)
	// wait until message flushed into GCS file.
	fp.flushLock.Lock()
	defer fp.flushLock.Unlock()
	err := <-fp.flushCh
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

func NewFirehose(storageBackend StorageBackend, consumerSettings gcp.Settings, processSettings ProcessSettings) (*Firehose, error) {
	// If we want to inject config from env, pass in when creating new

	// TODO: add logger here
	getLoggerFunc := func(_ context.Context) hedwig.Logger {
		return nil
	}
	backend := gcp.NewBackend(consumerSettings, getLoggerFunc)
	// TODO: add from schema generation here
	encoderDecoder, err := protobuf.NewMessageEncoderDecoder([]proto.Message{})
	if err != nil {
		return nil, err
	}
	// TODO: register same callback with all messages to basically write to memory until flush
	registry := hedwig.CallbackRegistry{}

	hedwigConsumer := hedwig.NewQueueConsumer(backend, encoderDecoder, getLoggerFunc, registry)
	hedwigFirehose := hedwig.NewFirehose(encoderDecoder, encoderDecoder)
	f := &Firehose{
		processSettings: processSettings,
		storageBackend:  storageBackend,
		hedwigConsumer:  hedwigConsumer,
		hedwigFirehose:  hedwigFirehose,
	}
	return f, nil
}

func main() {
	fmt.Println("Hello World")
}

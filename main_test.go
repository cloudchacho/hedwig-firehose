package main

import (
	"fmt"
	"context"
	"testing"
	"time"
	"regexp"
	"strings"

	firehoseGcp "github.com/cloudchacho/hedwig-firehose/gcp"
	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/stretchr/testify/mock"
	"github.com/Masterminds/semver"


	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"google.golang.org/protobuf/proto"
	// "google.golang.org/protobuf/reflect/protoreflect"
	// "google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/api/iterator"
)

var schemaRegex *regexp.Regexp

func init() {
	schemaRegex = regexp.MustCompile(`([^/]+)/(\d+)\.(\d+)$`)
}

type GcpTestSuite struct {
	suite.Suite
	storageClient  *storage.Client
	server         *fakestorage.Server
	sampleSettings firehoseGcp.Settings
	pubSubSettings gcp.Settings
	pubSubClient   *pubsub.Client
}

type firehoseConsumerDecoder struct {
	*protobuf.EncoderDecoder
}

//
//// DecodeMessageType decodes message type from meta attributes
//func (fcd *firehoseConsumerDecoder) DecodeMessageType(schema string) (string, *semver.Version, error) {
//	m := schemaRegex.FindStringSubmatch(schema)
//	if len(m) == 0 {
//		return "", nil, errors.Errorf("invalid schema: '%s' doesn't match valid regex", schema)
//	}
//
//	versionStr := fmt.Sprintf("%s.%s", m[2], m[3])
//	version, err := semver.NewVersion(versionStr)
//	if err != nil {
//		// would never happen
//		return "", nil, errors.Errorf("unable to parse as version: %s", versionStr)
//	}
//	return m[1], version, nil
//}
//
//// ExtractData extracts data from the on-the-wire payload
//// Type of data will be *anypb.Any
//func (fcd *firehoseConsumerDecoder) ExtractData(messagePayload []byte, attributes map[string]string) (hedwig.MetaAttributes, interface{}, error) {
//	userCreatedPayload := UserCreatedV1{}
//	err := proto.Unmarshal(messagePayload, &userCreatedPayload)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("typeurl", *userCreatedPayload.UserId)
//	metaAttrs := hedwig.MetaAttributes{}
//	payload := protobuf.PayloadV1{}
//	err = proto.Unmarshal(messagePayload, &payload)
//	if err != nil {
//		return metaAttrs, nil, errors.Wrap(err, "Unexpected data couldn't be unmarshaled")
//	}
//	fmt.Println("typeurl", payload.Data.TypeUrl)
//	fmt.Println("hi", payload.FormatVersion)
//	formatVersion, err := semver.NewVersion(payload.FormatVersion)
//	if err != nil {
//		return metaAttrs, nil, errors.Wrap(err, "Unexpected data: invalid format version")
//	}
//
//	metaAttrs.Timestamp = payload.Metadata.Timestamp.AsTime()
//	metaAttrs.Publisher = payload.Metadata.Publisher
//	metaAttrs.Headers = payload.Metadata.Headers
//	metaAttrs.ID = payload.Id
//	metaAttrs.Schema = payload.Schema
//	metaAttrs.FormatVersion = formatVersion
//
//	return metaAttrs, payload.Data, nil
//}

func (fcd *firehoseConsumerDecoder) DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	return data, nil
}

type firehoseStorageEncoder struct {
}

// EncodeData encodes the message with appropriate format for transport over the wire
// Type of data must be proto.Message
func (fcd *firehoseStorageEncoder) EncodeData(data interface{}, useMessageTransport bool, metaAttrs hedwig.MetaAttributes) ([]byte, error) {
	if useMessageTransport {
		panic("invalid input")
	}
	const urlPrefix = "type.googleapis.com/"
	dst := anypb.Any{}
	// TODO take this as an input to struct
	dst.TypeUrl = urlPrefix + string("standardbase.hedwig.UserCreatedV1")
	dst.Value = data.([]byte)
	container := &protobuf.PayloadV1{
		FormatVersion: fmt.Sprintf("%d.%d", metaAttrs.FormatVersion.Major(), metaAttrs.FormatVersion.Minor()),
		Id:            metaAttrs.ID,
		Metadata: &protobuf.MetadataV1{
			Publisher: metaAttrs.Publisher,
			Timestamp: timestamppb.New(metaAttrs.Timestamp),
			Headers:   metaAttrs.Headers,
		},
		Schema: metaAttrs.Schema,
		Data:   &dst,
	}
	payload, err := proto.Marshal(container)
	if err != nil {
		// Unable to convert to bytes
		return nil, err
	}
	return payload, nil
}

// VerifyKnownMinorVersion checks that message version is known to us
func (fcd *firehoseStorageEncoder) VerifyKnownMinorVersion(messageType string, version *semver.Version) error {
	// no minor verification
	return nil
}

// EncodeMessageType encodes the message type with appropriate format for transport over the wire
func (fcd *firehoseStorageEncoder) EncodeMessageType(messageType string, version *semver.Version) string {
	return fmt.Sprintf("%s/%d.%d", messageType, version.Major(), version.Minor())
}

func (fcd *firehoseStorageEncoder) IsBinary() bool {
	return true
}

// type Decoder interface {
// 	// DecodeData validates and decodes data
// 	DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error)

// 	// ExtractData extracts data from the on-the-wire payload when not using message transport
// 	ExtractData(messagePayload []byte, attributes map[string]string) (hedwig.MetaAttributes, interface{}, error)

// 	// DecodeMessageType decodes message type from meta attributes
// 	DecodeMessageType(schema string) (string, *semver.Version, error)
// }

// // Encoder is responsible for encoding the message payload in appropriate format for over the wire transport
// type Encoder interface {
// 	// EncodeData encodes the message with appropriate format for transport over the wire
// 	EncodeData(data interface{}, useMessageTransport bool, metaAttrs MetaAttributes) ([]byte, error)

// 	// EncodeMessageType encodes the message type with appropriate format for transport over the wire
// 	EncodeMessageType(messageType string, version *semver.Version) string

// 	// VerifyKnownMinorVersion checks that message version is known to us
// 	VerifyKnownMinorVersion(messageType string, version *semver.Version) error

// 	// True if encoding format is binary
// 	IsBinary() bool
// }

func (s *GcpTestSuite) SetupSuite() {
	s.TearDownSuite()
	ctx := context.Background()
	if s.pubSubClient == nil {
		client, err := pubsub.NewClient(ctx, "emulator-project")
		s.Require().NoError(err)
		s.pubSubClient = client
	}
		dlqTopic, err := s.pubSubClient.CreateTopic(ctx, "hedwig-dev-myapp-dlq")
	s.Require().NoError(err)
	_, err = s.pubSubClient.CreateSubscription(ctx, "hedwig-dev-myapp-dlq", pubsub.SubscriptionConfig{
		Topic:       dlqTopic,
		AckDeadline: time.Second * 20,
	})
	s.Require().NoError(err)
	topic, err := s.pubSubClient.CreateTopic(ctx, "hedwig-dev-user-created-v1")
	s.Require().NoError(err)
	_, err = s.pubSubClient.CreateSubscription(ctx, "hedwig-dev-myapp-dev-user-created-v1", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Second * 20,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: 5,
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(err)
	_, err = s.pubSubClient.CreateSubscription(ctx, "hedwig-dev-myapp-other-project-dev-user-created-v1", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Second * 20,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: 5,
		},
	})
	s.Require().NoError(err)
	topic, err = s.pubSubClient.CreateTopic(ctx, "hedwig-dev-myapp")
	s.Require().NoError(err)
	_, err = s.pubSubClient.CreateSubscription(ctx, "hedwig-dev-myapp", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Second * 20,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: 5,
		},
	})
	s.Require().NoError(err)
}

func (s *GcpTestSuite) TearDownSuite() {
	ctx := context.Background()
	if s.pubSubClient == nil {
		client, err := pubsub.NewClient(ctx, "emulator-project")
		s.Require().NoError(err)
		s.pubSubClient = client
	}
	defer func() {
		s.Require().NoError(s.pubSubClient.Close())
		s.pubSubClient = nil
	}()
	subscriptions := s.pubSubClient.Subscriptions(ctx)
	for {
		if subscription, err := subscriptions.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to delete subscriptions with error: %v", err))
		} else {
			err = subscription.Delete(ctx)
			s.Require().NoError(err)
		}
	}
	topics := s.pubSubClient.Topics(ctx)
	for {
		if topic, err := topics.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to delete topics with error: %v", err))
		} else {
			err = topic.Delete(ctx)
			s.Require().NoError(err)
		}
	}
}

func (s *GcpTestSuite) BeforeTest(suiteName, testName string) {
	s.server = fakestorage.NewServer([]fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-staging-bucket",
				Name:       "some/object/file.txt",
			},
			Content: []byte("inside the file"),
		},
	})
	// s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{
	// 	Name: "some-staging-bucket",
	// 	VersioningEnabled: true,
	// })
	s.storageClient = s.server.Client()
	s.sampleSettings = firehoseGcp.Settings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket:  "some-staging-bucket",
		OutputBucket:   "some-output-bucket",
	}
	settings := gcp.Settings{
		GoogleCloudProject: "emulator-project",
		QueueName:          "dev-myapp",
		Subscriptions:      []string{"dev-user-created-v1"},
	}
	s.pubSubSettings = settings
}

func (s *GcpTestSuite) AfterTest(suiteName, testName string) {
	s.server.Stop()
}

func (s *GcpTestSuite) TestNewFirehose() {
	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{"user-created", 1}}
	var s3 ProcessSettings
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(firehoseGcp.Settings{}, &storage.Client{})
	encoderdecoder, _ := protobuf.NewMessageEncoderDecoder([]proto.Message{})
	f, err := NewFirehose(backend, encoderdecoder, encoderdecoder, msgList, storageBackend, s2, s3, hedwigLogger)
	assert.Equal(s.T(), nil, err)
	assert.NotNil(s.T(), f)
}

type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

type fakeSerializer struct {
	mock.Mock
}

func (s *GcpTestSuite) TestFirehoseIntegration() {
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(s.pubSubSettings, hedwigLogger)
	// maybe just user-created?
	msgList := []hedwig.MessageTypeMajorVersion{{"user-created", 1}}
	s3 := ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket:  "some-staging-bucket",
		OutputBucket:   "some-output-bucket",
		FlushAfter: 2,
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.sampleSettings, s.storageClient)
	storageEncoder := firehoseStorageEncoder{}
	decoder := firehoseConsumerDecoder{}
	f, err := NewFirehose(backend, &storageEncoder, &decoder, msgList, storageBackend, s2, s3, hedwigLogger)

	routing := map[hedwig.MessageTypeMajorVersion]string{
		{
			MessageType:  "user-created",
			MajorVersion: 1,
		}: "dev-user-created-v1",
	}
	pubEncoderDecoder, err := protobuf.NewMessageEncoderDecoder(
		[]proto.Message{&UserCreatedV1{}},
	)
	publisher := hedwig.NewPublisher(backend, pubEncoderDecoder, routing)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	defer cancel()
	// numMessages := uint32(10)
	// visibilityTimeout := time.Second * 10

	userId := "C_1234567890123456"
	data := UserCreatedV1{UserId: &userId}
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &data, "myapp")
	_, err = publisher.Publish(ctx, message)
	s.Require().NoError(err)

	_, err = publisher.Publish(ctx, message)
	s.Require().NoError(err)

	fmt.Println("running follower")
	go f.RunFollower(ctx)

	// stop test after 5sec, should finish by then
	timerCh := time.After(5 * time.Second)
outer:
	for {
		select {
		case <-timerCh:
			fmt.Println("time out")
			it := s.storageClient.Bucket("some-staging-bucket").Objects(context.Background(), nil)
			userCreatedObjs := []string{}
			for {
				attrs, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					fmt.Println("problem next", err)
				}
				if strings.Contains(attrs.Name, "user-created/1") {
					userCreatedObjs = append(userCreatedObjs, attrs.Name)
				}
				fmt.Println(attrs.Name)
			}
			fmt.Println("objs in gcs err ", err)
			assert.Equal(s.T(), 2, len(userCreatedObjs))
			break outer
		}
	}


	// other stuff I may need
	// ctx2, cancel2 := context.WithCancel(ctx)
	// err = s.client.Subscription("hedwig-dev-myapp-dev-user-created-v1").Receive(ctx2, func(_ context.Context, message *pubsub.Message) {
	// 	defer cancel2()
	// 	s.Equal(message.Data, s.payload)
	// 	s.Equal(message.Attributes, s.attributes)
	// 	message.Ack()
	// })
	// s.NoError(err)

	// ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	// defer cancel()
	// testutils.RunAndWait(func() {
	// 	err := s.client.Subscription("hedwig-dev-myapp-dev-user-created-v1").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
	// 		s.Fail("shouldn't have received any message")
	// 	})
	// 	s.Require().NoError(err)
	// })
}


// func (s *GcpTestSuite) TestPublish() {
// 	var hedwigLogger hedwig.Logger
// 	backend := gcp.NewBackend(s.pubSubSettings, hedwigLogger)
// 	ctx, cancel := context.WithCancel(context.Background())

// 	msgTopic := "dev-user-created-v1"
// 	payload := []byte(`{"foo": "bar"}`)
// 	attributes := map[string]string{
// 		"foo": "bar",
// 	}
// 	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &fakeHedwigDataField{}, "myapp")

// 	messageID, err := backend.Publish(ctx, message, payload, attributes, msgTopic)
// 	s.NoError(err)
// 	s.NotEmpty(messageID)

// 	err = s.pubSubClient.Subscription("hedwig-dev-myapp-dev-user-created-v1").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
// 		cancel()
// 		s.Equal(message.Data, payload)
// 		s.Equal(message.Attributes, attributes)
// 		message.Ack()
// 	})
// 	s.Require().NoError(err)
// }

func TestGcpTestSuite(t *testing.T) {
	suite.Run(t, new(GcpTestSuite))
}

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	firehoseGcp "github.com/cloudchacho/hedwig-firehose/gcp"
	firehoseProtobuf "github.com/cloudchacho/hedwig-firehose/protobuf"
	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

type GcpTestSuite struct {
	suite.Suite
	storageClient  *storage.Client
	server         *fakestorage.Server
	pubSubSettings gcp.Settings
	pubSubClient   *pubsub.Client
}

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
	s.storageClient = s.server.Client()
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
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	var s3 ProcessSettings
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(&storage.Client{})
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	f, err := NewFirehose(backend, &encoderDecoder, msgList, storageBackend, s2, s3, hedwigLogger)
	assert.Equal(s.T(), nil, err)
	assert.NotNil(s.T(), f)
}

func (s *GcpTestSuite) TestFirehoseFollowerIntegration() {
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(s.pubSubSettings, hedwigLogger)
	// maybe just user-created?
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket:  "some-staging-bucket",
		OutputBucket:   "some-output-bucket",
		FlushAfter:     5,
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	msgTypeUrls := map[hedwig.MessageTypeMajorVersion]string{
		{MessageType: "user-created", MajorVersion: 1}: "type.googleapis.com/standardbase.hedwig.UserCreatedV1",
	}
	encoderDecoder := firehoseProtobuf.NewFirehoseEncodeDecoder(msgTypeUrls)
	f, err := NewFirehose(backend, encoderDecoder, msgList, storageBackend, s2, s3, hedwigLogger)
	s.Require().NoError(err)
	// freeze time for test
	parsed, _ := time.Parse("2006-01-02", "2022-10-15")
	c := Clock{instant: parsed}
	f.clock = &c

	routing := map[hedwig.MessageTypeMajorVersion]string{
		{
			MessageType:  "user-created",
			MajorVersion: 1,
		}: "dev-user-created-v1",
	}
	pubEncoderDecoder, err := protobuf.NewMessageEncoderDecoder(
		[]proto.Message{&UserCreatedV1{}},
	)
	s.Require().NoError(err)
	publisher := hedwig.NewPublisher(backend, pubEncoderDecoder, routing)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

	defer cancel()

	userId := "C_1234567890123456"
	data := UserCreatedV1{UserId: &userId}
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &data, "myapp")
	s.Require().NoError(err)
	_, err = publisher.Publish(ctx, message)
	s.Require().NoError(err)

	userId2 := "C_9012345678901234"
	data2 := UserCreatedV1{UserId: &userId2}
	message2, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo2": "bar2"}, &data2, "myapp")
	s.Require().NoError(err)
	_, err = publisher.Publish(ctx, message2)
	s.Require().NoError(err)

	go f.RunFollower(ctx)

	// stop test after 5sec, should finish by then
	timerCh := time.After(5 * time.Second)
	<-timerCh
	it := s.storageClient.Bucket("some-staging-bucket").Objects(context.Background(), nil)
	userCreatedObjs := []string{}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		s.Require().NoError(err)
		// check that file under message folder
		if strings.Contains(attrs.Name, "user-created/1") {
			userCreatedObjs = append(userCreatedObjs, attrs.Name)
			r, _ := f.storageBackendCreator.CreateReader(ctx, "some-staging-bucket", attrs.Name)
			res, err := f.hedwigFirehose.Deserialize(r)
			fmt.Println(res)
			s.Require().NoError(err)
		}
		fmt.Println(attrs.Name)
	}
	assert.Equal(s.T(), 1, len(userCreatedObjs))
}

func TestGcpTestSuite(t *testing.T) {
	suite.Run(t, new(GcpTestSuite))
}

package firehose_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudchacho/hedwig-firehose"
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

func GetSubName(msg *hedwig.Message) (string, error) {
	return msg.Metadata.ProviderMetadata.(gcp.Metadata).SubscriptionName, nil
}

type GcpTestSuite struct {
	suite.Suite
	filePrefixes   []string
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

func (s *GcpTestSuite) SetupTest() {
	s.server = fakestorage.NewServer([]fakestorage.Object{})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "some-staging-bucket"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "some-output-bucket"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "some-metadata-bucket"})
	s.filePrefixes = []string{"dev-myapp-dev-user-created-v1"}
	s.storageClient = s.server.Client()
	settings := gcp.Settings{
		GoogleCloudProject: "emulator-project",
		QueueName:          "dev-myapp",
		Subscriptions:      []string{"dev-user-created-v1"},
	}
	s.pubSubSettings = settings
}

func (s *GcpTestSuite) TearDownTest() {
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
	var s3 firehose.ProcessSettings
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(&storage.Client{})
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: firehose.DefaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	assert.Equal(s.T(), nil, err)
	assert.NotNil(s.T(), f)
}

func (s *GcpTestSuite) TestFollowerCtxDone() {
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(s.pubSubSettings, hedwigLogger)
	// maybe just user-created?
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket:  "some-staging-bucket",
		OutputBucket:   "some-output-bucket",
		FlushAfter:     2,
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	msgTypeUrls := map[hedwig.MessageTypeMajorVersion]string{
		{MessageType: "user-created", MajorVersion: 1}: "type.googleapis.com/standardbase.hedwig.UserCreatedV1",
	}
	encoderDecoder := firehoseProtobuf.NewFirehoseEncodeDecoder(msgTypeUrls)
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: firehose.DefaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}
	f, err := firehose.NewFirehose(backend, encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)

	contextTimeout := time.Second * 1
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

	defer cancel()
	userId := "C_1234567890123456"
	data := firehose.UserCreatedV1{UserId: &userId}
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &data, "myapp")
	s.Require().NoError(err)
	routing := map[hedwig.MessageTypeMajorVersion]string{
		{
			MessageType:  "user-created",
			MajorVersion: 1,
		}: "dev-user-created-v1",
	}
	pubEncoderDecoder, err := protobuf.NewMessageEncoderDecoder(
		[]proto.Message{&firehose.UserCreatedV1{}},
	)
	s.Require().NoError(err)
	publisher := hedwig.NewPublisher(backend, pubEncoderDecoder, routing)
	_, err = publisher.Publish(ctx, message)
	s.Require().NoError(err)

	_ = f.RunFollower(ctx)
}

func (s *GcpTestSuite) TestFollowerPanicBadMsgToFilePrefix() {
	defer func() {
		if r := recover(); r == nil {
			s.T().Errorf("The code did not panic")
		}
	}()

	badFunc := func(message *hedwig.Message) (string, error) {
		return "not_a_file_prefix", nil
	}

	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	var s3 firehose.ProcessSettings
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(&storage.Client{})
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: firehose.DefaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}

	contextTimeout := time.Second * 30
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

	defer cancel()
	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, badFunc, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)
	_ = f.RunFollower(ctx)
}

func (s *GcpTestSuite) TestFollowerPanic() {
	defer func() {
		if r := recover(); r == nil {
			s.T().Errorf("The code did not panic")
		}
	}()

	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	var s3 firehose.ProcessSettings
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(&storage.Client{})
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: firehose.DefaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}

	contextTimeout := time.Second * 30
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

	defer cancel()
	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)
	_ = f.RunFollower(ctx)
}

func (s *GcpTestSuite) TestFirehoseDoesNotRunDeploymentDoesNotMatch() {
	instance := "instance_1"
	deployment := "deployment_2"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-metadata-bucket",
				Name:       "leader.json",
			},
			Content: []byte("{\"deploymentId\": \"deployment_1\", \"nodeId\": \"instance_2\", \"timestamp\": \"1658342551\"}"),
		},
	)
	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket:     "some-metadata-bucket",
		AcquireRoleTimeout: 5,
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().Nil(err)

	// set longer than global timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	assert.Equal(s.T(), fmt.Errorf("failed to read leader file or deploymentId did not match"), f.RunFirehose(ctx))
}

func (s *GcpTestSuite) TestFirehoseInFollowerMode() {
	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-metadata-bucket",
				Name:       "leader.json",
			},
			Content: []byte("{\"deploymentId\": \"deployment_1\", \"nodeId\": \"instance_2\", \"timestamp\": \"1658342551\"}"),
		},
	)
	s.RunFirehoseFollowerIntegration()
}

func (s *GcpTestSuite) RunFirehoseFollowerIntegration() {
	hedwigLogger := hedwig.StdLogger{}
	backend := gcp.NewBackend(s.pubSubSettings, hedwigLogger)
	// maybe just user-created?
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket:  "some-staging-bucket",
		OutputBucket:   "some-output-bucket",
		FlushAfter:     2,
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	msgTypeUrls := map[hedwig.MessageTypeMajorVersion]string{
		{MessageType: "user-created", MajorVersion: 1}: "type.googleapis.com/standardbase.hedwig.UserCreatedV1",
	}
	encoderDecoder := firehoseProtobuf.NewFirehoseEncodeDecoder(msgTypeUrls)
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: firehose.DefaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}
	f, err := firehose.NewFirehose(backend, encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)
	// freeze time for test
	parsed, _ := time.Parse("2006-01-02", "2022-10-15")
	c := firehose.Clock{Instant: parsed}
	f.Clock = &c

	routing := map[hedwig.MessageTypeMajorVersion]string{
		{
			MessageType:  "user-created",
			MajorVersion: 1,
		}: "dev-user-created-v1",
	}
	pubEncoderDecoder, err := protobuf.NewMessageEncoderDecoder(
		[]proto.Message{&firehose.UserCreatedV1{}},
	)
	s.Require().NoError(err)
	publisher := hedwig.NewPublisher(backend, pubEncoderDecoder, routing)

	contextTimeout := time.Second * 30
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

	defer cancel()

	userId := "C_1234567890123456"
	data := firehose.UserCreatedV1{UserId: &userId}
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &data, "myapp")
	s.Require().NoError(err)
	_, err = publisher.Publish(ctx, message)
	s.Require().NoError(err)

	userId2 := "C_9012345678901234"
	data2 := firehose.UserCreatedV1{UserId: &userId2}
	message2, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar2"}, &data2, "myapp")
	s.Require().NoError(err)
	_, err = publisher.Publish(ctx, message2)
	s.Require().NoError(err)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		err := f.RunFirehose(ctx)
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			s.T().FailNow()
		}
	}()

outer:
	for {
		select {
		case <-ctx.Done():
			s.T().FailNow()
		default:
			// poll for file every 2 seconds
			<-time.After(time.Second * 2)
			attrs, err := s.storageClient.Bucket("some-staging-bucket").Object("dev-myapp-dev-user-created-v1/instance_1/2022/10/15/1665792000").Attrs(ctx)
			if err == storage.ErrObjectNotExist {
				continue
			}
			r, err := f.StorageBackend.CreateReader(context.Background(), "some-staging-bucket", attrs.Name)
			defer r.Close()
			s.Require().NoError(err)
			_, err = f.HedwigFirehose.Deserialize(r)
			// keep trying if file can not be deserialized
			if err != nil {
				continue
			}
			// wait one second to continue test after file detected
			<-time.After(time.Second * 1)
			break outer
		}
	}

	it := s.storageClient.Bucket("some-staging-bucket").Objects(context.Background(), nil)
	userCreatedObjs := []string{}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		s.Require().NoError(err)
		// check that file under message folder
		if attrs.Name == "dev-myapp-dev-user-created-v1/instance_1/2022/10/15/1665792000" {
			userCreatedObjs = append(userCreatedObjs, attrs.Name)
			r, err := f.StorageBackend.CreateReader(context.Background(), "some-staging-bucket", attrs.Name)
			defer r.Close()
			s.Require().NoError(err)
			res, err := f.HedwigFirehose.Deserialize(r)
			s.Require().NoError(err)
			assert.Equal(s.T(), 2, len(res))
			foundMetaData := map[string]int{"bar": 0, "bar2": 0}
			for _, r := range res {
				foundMetaData[r.Metadata.Headers["foo"]]++
			}
			assert.Equal(s.T(), foundMetaData, map[string]int{"bar": 1, "bar2": 1})
		}
	}
	assert.Equal(s.T(), 1, len(userCreatedObjs))
	cancel()
}

func (s *GcpTestSuite) TestFirehoseInLeaderMode() {
	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-metadata-bucket",
				Name:       "leader.json",
			},
			Content: []byte("{\"deploymentId\": \"deployment_1\", \"nodeId\": \"instance_1\", \"timestamp\": \"1658342551\"}"),
		},
	)
	s.RunFirehoseLeaderIntegration()
	_, err := s.server.GetObject("some-metadata-bucket", "leader.json")
	assert.Equal(s.T(), err.Error(), "object not found")
}

func (s *GcpTestSuite) userCreatedMessage(
	f *firehose.Firehose,
	userId string,
	foo string,
	timestamp time.Time,
) *hedwig.Message {
	data := firehose.UserCreatedV1{UserId: &userId}
	mData, err := proto.Marshal(&data)
	s.Require().NoError(err)
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": foo}, mData, "myapp")
	s.Require().NoError(err)
	message.Metadata.Timestamp = timestamp

	return message
}

func (s *GcpTestSuite) serialize(f *firehose.Firehose, message *hedwig.Message) []byte {
	serialized, err := f.HedwigFirehose.Serialize(message)
	s.Require().NoError(err)
	expMsgLength := new(bytes.Buffer)
	_ = binary.Write(expMsgLength, binary.LittleEndian, len(serialized))
	return append(expMsgLength.Bytes(), serialized...)
}

func (s *GcpTestSuite) RunFirehoseLeaderIntegration() {
	hedwigLogger := hedwig.StdLogger{}
	backend := gcp.NewBackend(s.pubSubSettings, hedwigLogger)
	// maybe just user-created?
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket:  "some-staging-bucket",
		OutputBucket:   "some-output-bucket",
		FlushAfter:     2,
		ScrapeInterval: 1,
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	msgTypeUrls := map[hedwig.MessageTypeMajorVersion]string{
		{MessageType: "user-created", MajorVersion: 1}: "type.googleapis.com/standardbase.hedwig.UserCreatedV1",
	}
	encoderDecoder := firehoseProtobuf.NewFirehoseEncodeDecoder(msgTypeUrls)
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: firehose.DefaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}
	f, err := firehose.NewFirehose(backend, encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)
	// freeze time for test
	parsed, _ := time.Parse("2006-01-02", "2022-10-16")
	c := firehose.Clock{Instant: parsed}
	f.Clock = &c

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	defer cancel()

	// Set message timestamps to be 2022/10/15 (a day before the time we set above)
	userId := "C_1234567890123456"
	msg1 := s.userCreatedMessage(f, userId, "bar", time.Date(2022, time.Month(10), 15, 0, 0, 0, 0, time.UTC))
	expected1 := s.serialize(f, msg1)

	// Next message sent 10 seconds later
	userId2 := "C_9012345678901234"
	msg2 := s.userCreatedMessage(f, userId2, "bar2", time.Date(2022, time.Month(10), 15, 0, 0, 10, 0, time.UTC))
	expected2 := s.serialize(f, msg2)

	// Next message sent another 10 seconds later
	userId3 := "C_0123456789012345"
	msg3 := s.userCreatedMessage(f, userId3, "bar3", time.Date(2022, time.Month(10), 15, 0, 0, 20, 0, time.UTC))
	expected3 := s.serialize(f, msg3)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "some-staging-bucket",
			Name:       "dev-myapp-dev-user-created-v1/1/2022/10/15/1665792000",
		},
		Content: expected1,
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "some-staging-bucket",
			Name:       "dev-myapp-dev-user-created-v1/1/2022/10/15/1665792010",
		},
		Content: expected2,
	})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		err := f.RunFirehose(ctx)
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			s.T().FailNow()
		}
		// after ctx cancel, leader json should no longer exist
		_, err = s.server.GetObject("some-metadata-bucket", "leader.json")
		assert.NotNil(s.T(), err)
	}()

	// Once the firehose has processed the existing staging files, give it a new one to process.
	wg.Add(1)
	go func() {
		defer wg.Done()

	outer:
		for {
			select {
			case <-ctx.Done():
				fmt.Println("ctx timeout")
				s.T().FailNow()
			default:
				// poll for file every 2 seconds
				<-time.After(time.Second * 2)

				// Note that even though "now" is 2022/10/16, the object path is based
				// on the timestamp in the message itself (2022/10/15)
				expectedPath := fmt.Sprintf("dev-myapp-dev-user-created-v1/2022/10/15/%d_%s", msg2.Metadata.Timestamp.Unix(), msg2.ID)
				_, err := s.storageClient.Bucket("some-output-bucket").Object(expectedPath).Attrs(ctx)
				if err == storage.ErrObjectNotExist {
					continue
				}

				// Update the firehose's clock and add another message
				parsed, _ := time.Parse("2006-01-02 15:04", "2022-10-16 00:10")
				f.Clock.Change(parsed)
				// Wait at least one scrape interval for the firehose to re-read the clock
				<-time.After(time.Second * 2)

				s.server.CreateObject(fakestorage.Object{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "some-staging-bucket",
						Name:       "dev-myapp-dev-user-created-v1/1/2022/10/15/1665792020",
					},
					Content: expected3,
				})
				break outer
			}
		}
	}()

outer:
	for {
		select {
		case <-ctx.Done():
			fmt.Println("ctx timeout")
			s.T().FailNow()
		default:
			// poll for file every 2 seconds
			<-time.After(time.Second * 2)
			expectedPath := fmt.Sprintf("dev-myapp-dev-user-created-v1/2022/10/15/%d_%s", msg3.Metadata.Timestamp.Unix(), msg3.ID)
			_, err := s.storageClient.Bucket("some-output-bucket").Object(expectedPath).Attrs(ctx)
			if err == storage.ErrObjectNotExist {
				continue
			}
			// wait one second to continue test after file detected
			<-time.After(time.Second * 1)
			break outer
		}
	}

	it := s.storageClient.Bucket("some-output-bucket").Objects(context.Background(), nil)
	userCreatedObjs := []string{}
	foundIndex := false
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		s.Require().NoError(err)

		// check the contents of the first output file, which should contain `msg1` and `msg2`
		expectedPath := fmt.Sprintf("dev-myapp-dev-user-created-v1/2022/10/15/%d_%s", msg2.Metadata.Timestamp.Unix(), msg2.ID)
		if attrs.Name == expectedPath {
			userCreatedObjs = append(userCreatedObjs, attrs.Name)
			r, err := f.StorageBackend.CreateReader(ctx, "some-output-bucket", attrs.Name)
			defer r.Close()
			s.Require().NoError(err)
			res, err := f.HedwigFirehose.Deserialize(r)
			// check errors
			s.Require().NoError(err)
			assert.Equal(s.T(), 2, len(res))
			foundMetaData := map[string]int{"bar": 0, "bar2": 0}
			var lastTimestamp int64 = 0
			for _, r := range res {
				// assert timestamps are increasing
				assert.Greater(s.T(), r.Metadata.Timestamp.Unix(), lastTimestamp)
				lastTimestamp = r.Metadata.Timestamp.Unix()
				foundMetaData[r.Metadata.Headers["foo"]]++
			}
			assert.Equal(s.T(), foundMetaData, map[string]int{"bar": 1, "bar2": 1})
		}

		if attrs.Name == "dev-myapp-dev-user-created-v1/2022/10/15/_metadata.ndjson" {
			foundIndex = true
			r, err := f.StorageBackend.CreateReader(ctx, "some-output-bucket", attrs.Name)
			defer r.Close()
			s.Require().NoError(err)
			res, err := io.ReadAll(r)
			s.Require().NoError(err)
			lines := strings.Split(string(res[:]), "\n")

			// Check each line of the index file
			actual0 := indexFile{}
			json.Unmarshal([]byte(lines[0]), &actual0)
			assert.Equal(s.T(), actual0, indexFile{
				Name:         fmt.Sprintf("%d_%s", msg2.Metadata.Timestamp.Unix(), msg2.ID),
				MinTimestamp: msg1.Metadata.Timestamp.Unix(),
				MaxTimestamp: msg2.Metadata.Timestamp.Unix(),
			})

			actual1 := indexFile{}
			json.Unmarshal([]byte(lines[1]), &actual1)
			assert.Equal(s.T(), actual1, indexFile{
				Name:         fmt.Sprintf("%d_%s", msg3.Metadata.Timestamp.Unix(), msg3.ID),
				MinTimestamp: msg3.Metadata.Timestamp.Unix(),
				MaxTimestamp: msg3.Metadata.Timestamp.Unix(),
			})

			assert.Equal(s.T(), len(lines), 3) // 3, because of the empty line at the end
		}
	}
	assert.Equal(s.T(), 1, len(userCreatedObjs))
	assert.True(s.T(), foundIndex)
	cancel()
}

type indexFile struct {
	Name         string `json:"name"`
	MinTimestamp int64  `json:"min_timestamp"`
	MaxTimestamp int64  `json:"max_timestamp"`
}

func (s *GcpTestSuite) TestIsLeaderFileBadFormat() {
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-metadata-bucket",
				Name:       "leader.json",
			},
			Content: []byte("not a json string"),
		},
	)
	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)

	res, err := f.IsLeader(ctx)
	assert.Equal(s.T(), res, false)
	assert.Equal(s.T(), err.Error(), "invalid character 'o' in literal null (expecting 'u')")
}

func (s *GcpTestSuite) TestIsLeaderNoDeploymentId() {
	defer func() {
		if r := recover(); r == nil {
			s.T().Errorf("The code did not panic")
		}
	}()

	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	os.Setenv("GAE_INSTANCE", "")
	os.Setenv("GAE_DEPLOYMENT_ID", "")

	_, _ = f.IsLeader(ctx)
}

func (s *GcpTestSuite) TestIsLeaderNoFile() {
	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)

	res, err := f.IsLeader(ctx)
	s.Require().Nil(err)
	assert.Equal(s.T(), res, true)

	r, err := f.StorageBackend.CreateReader(ctx, s3.MetadataBucket, "leader.json")
	s.Require().Nil(err)
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	s.Require().Nil(err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), result["DeploymentId"], deployment)
	assert.Equal(s.T(), result["NodeId"], instance)
}

func (s *GcpTestSuite) TestIsLeaderMatchingNode() {
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-metadata-bucket",
				Name:       "leader.json",
			},
			Content: []byte("{\"deploymentId\": \"deployment_1\", \"nodeId\": \"instance_1\", \"timestamp\": \"1658342551\"}"),
		},
	)

	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)

	res, err := f.IsLeader(ctx)
	s.Require().Nil(err)
	assert.Equal(s.T(), res, true)
}

func (s *GcpTestSuite) TestIsLeaderDeploymentDoesntMatch() {
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-metadata-bucket",
				Name:       "leader.json",
			},
			Content: []byte("{\"deploymentId\": \"deployment_2\", \"nodeId\": \"instance_1\", \"timestamp\": \"1658342551\"}"),
		},
	)

	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)

	res, err := f.IsLeader(ctx)
	assert.Equal(s.T(), res, false)
	assert.Equal(s.T(), err, fmt.Errorf("deploymentId does not match current leader"))
}

func (s *GcpTestSuite) TestNotLeader() {
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-metadata-bucket",
				Name:       "leader.json",
			},
			Content: []byte("{\"deploymentId\": \"deployment_1\", \"nodeId\": \"instance_2\", \"timestamp\": \"1658342551\"}"),
		},
	)

	gcpSettings := gcp.Settings{}
	var hedwigLogger hedwig.Logger
	backend := gcp.NewBackend(gcpSettings, hedwigLogger)
	msgList := []hedwig.MessageTypeMajorVersion{{
		MessageType:  "user-created",
		MajorVersion: 1,
	}}
	s3 := firehose.ProcessSettings{
		MetadataBucket: "some-metadata-bucket",
	}
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(s.storageClient)
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{}

	f, err := firehose.NewFirehose(backend, &encoderDecoder, msgList, s.filePrefixes, GetSubName, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().Nil(err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)

	res, err := f.IsLeader(ctx)
	s.Require().Nil(err)
	assert.Equal(s.T(), res, false)
}

func TestGcpTestSuite(t *testing.T) {
	suite.Run(t, new(GcpTestSuite))
}

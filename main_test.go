package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
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

func (s *GcpTestSuite) SetupTest() {
	s.server = fakestorage.NewServer([]fakestorage.Object{})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "some-staging-bucket"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "some-output-bucket"})
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
	var s3 ProcessSettings
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(&storage.Client{})
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: defaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}

	f, err := NewFirehose(backend, &encoderDecoder, msgList, storageBackend, lr, s2, s3, hedwigLogger)
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
	s3 := ProcessSettings{
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
		VisibilityTimeout: defaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}
	f, err := NewFirehose(backend, encoderDecoder, msgList, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)

	contextTimeout := time.Second * 1
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

	defer cancel()
	userId := "C_1234567890123456"
	data := UserCreatedV1{UserId: &userId}
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &data, "myapp")
	s.Require().NoError(err)
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
	_, err = publisher.Publish(ctx, message)
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
	var s3 ProcessSettings
	var s2 gcp.Settings
	storageBackend := firehoseGcp.NewBackend(&storage.Client{})
	encoderDecoder := firehoseProtobuf.FirehoseEncoderDecoder{}
	lr := hedwig.ListenRequest{
		NumMessages:       2,
		VisibilityTimeout: defaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}

	contextTimeout := time.Second * 30
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

	defer cancel()
	f, err := NewFirehose(backend, &encoderDecoder, msgList, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)
	_ = f.RunFollower(ctx)
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
		VisibilityTimeout: defaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}
	f, err := NewFirehose(backend, encoderDecoder, msgList, storageBackend, lr, s2, s3, hedwigLogger)
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

	contextTimeout := time.Second * 30
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

	defer cancel()

	userId := "C_1234567890123456"
	data := UserCreatedV1{UserId: &userId}
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, &data, "myapp")
	s.Require().NoError(err)
	_, err = publisher.Publish(ctx, message)
	s.Require().NoError(err)

	userId2 := "C_9012345678901234"
	data2 := UserCreatedV1{UserId: &userId2}
	message2, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar2"}, &data2, "myapp")
	s.Require().NoError(err)
	_, err = publisher.Publish(ctx, message2)
	s.Require().NoError(err)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		err := f.RunFollower(ctx)
		// should have errored due to ctx cancel
		s.Require().NotNil(err)
	}()

outer:
	for {
		select {
		case <-ctx.Done():
			s.T().FailNow()
		default:
			// poll for file every 2 seconds
			<-time.After(time.Second * 2)
			attrs, err := s.storageClient.Bucket("some-staging-bucket").Object("user-created/1/2022/10/15/1665792000").Attrs(ctx)
			if err == storage.ErrObjectNotExist {
				continue
			}
			r, err := f.storageBackend.CreateReader(context.Background(), "some-staging-bucket", attrs.Name)
			defer r.Close()
			s.Require().NoError(err)
			_, err = f.hedwigFirehose.Deserialize(r)
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
		if attrs.Name == "user-created/1/2022/10/15/1665792000" {
			userCreatedObjs = append(userCreatedObjs, attrs.Name)
			r, err := f.storageBackend.CreateReader(context.Background(), "some-staging-bucket", attrs.Name)
			defer r.Close()
			s.Require().NoError(err)
			res, err := f.hedwigFirehose.Deserialize(r)
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

func (s *GcpTestSuite) TestFirehoseLeaderIntegration() {
	hedwigLogger := hedwig.StdLogger{}
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
		VisibilityTimeout: defaultVisibilityTimeoutS,
		NumConcurrency:    2,
	}
	f, err := NewFirehose(backend, encoderDecoder, msgList, storageBackend, lr, s2, s3, hedwigLogger)
	s.Require().NoError(err)
	// freeze time for test
	parsed, _ := time.Parse("2006-01-02", "2022-10-16")
	c := Clock{instant: parsed}
	f.clock = &c

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	defer cancel()

	userId := "C_1234567890123456"
	data := UserCreatedV1{UserId: &userId}
	mData, err := proto.Marshal(&data)
	s.Require().NoError(err)
	message, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar"}, mData, "myapp")
	s.Require().NoError(err)

	userId2 := "C_9012345678901234"
	data2 := UserCreatedV1{UserId: &userId2}
	mData2, err := proto.Marshal(&data2)
	s.Require().NoError(err)
	// sleep for 1 sec to get timestamp 1 second later
	<-time.After(time.Second * 1)
	message2, err := hedwig.NewMessage("user-created", "1.0", map[string]string{"foo": "bar2"}, mData2, "myapp")
	s.Require().NoError(err)
	sm, err := f.hedwigFirehose.Serialize(message)
	s.Require().NoError(err)
	sm2, err := f.hedwigFirehose.Serialize(message2)
	s.Require().NoError(err)
	smL := len(sm)
	sm2L := len(sm2)

	expMsgLength := new(bytes.Buffer)
	_ = binary.Write(expMsgLength, binary.LittleEndian, smL)
	expected := append(expMsgLength.Bytes(), sm...)

	expMsgLength2 := new(bytes.Buffer)
	_ = binary.Write(expMsgLength2, binary.LittleEndian, sm2L)
	expected2 := append(expMsgLength2.Bytes(), sm2...)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "some-staging-bucket",
			Name:       "user-created/1/2022/10/15/1665792000",
		},
		Content: expected,
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "some-staging-bucket",
			Name:       "user-created/1/2022/10/15/1665792010",
		},
		Content: expected2,
	})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		f.RunLeader(ctx)
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
			_, err := s.storageClient.Bucket("some-output-bucket").Object("user-created/1/2022/10/16/user-created-1-1665878400.gz").Attrs(ctx)
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
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		s.Require().NoError(err)
		// check that file under message folder
		if attrs.Name == "user-created/1/2022/10/16/user-created-1-1665878400.gz" {
			userCreatedObjs = append(userCreatedObjs, attrs.Name)
			r, err := f.storageBackend.CreateReader(ctx, "some-output-bucket", attrs.Name)
			defer r.Close()
			s.Require().NoError(err)
			res, err := f.hedwigFirehose.Deserialize(r)
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
	}
	assert.Equal(s.T(), 1, len(userCreatedObjs))
	cancel()
}

func TestGcpTestSuite(t *testing.T) {
	suite.Run(t, new(GcpTestSuite))
}

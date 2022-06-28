package gcp_test

import (
	"context"
	"testing"
	"fmt"

	"google.golang.org/api/iterator"
	"cloud.google.com/go/storage"
	"github.com/cloudchacho/hedwig-firehose/gcp"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type GcpTestSuite struct {
	suite.Suite
	client         *storage.Client
	server         *fakestorage.Server
	sampleSettings gcp.Settings
}

func (s *GcpTestSuite) BeforeTest(suiteName, testName string) {
	s.server = fakestorage.NewServer([]fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-bucket",
				Name:       "some/object/file.txt",
			},
			Content: []byte("inside the file"),
		},
	})
	s.client = s.server.Client()
	s.sampleSettings = gcp.Settings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket:  "some-staging-bucket",
		OutputBucket:   "some-output-bucket",
	}
}

func (s *GcpTestSuite) AfterTest(suiteName, testName string) {
	s.server.Stop()
}

func (s *GcpTestSuite) TestRead() {
	b := gcp.Backend{
		GcsClient: s.client,
		Settings:  s.sampleSettings,
	}
	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/file.txt")
	assert.Equal(s.T(), nil, err)
	assert.Equal(s.T(), []byte("inside the file"), res)
}

func (s *GcpTestSuite) TestReadNotValidLocation() {
	b := gcp.Backend{
		GcsClient: s.client,
		Settings:  s.sampleSettings,
	}
	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/notthere.txt")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), res)
}

func (s *GcpTestSuite) TestUploadWriter() {
	b := gcp.Backend{
		GcsClient: s.client,
		Settings:  s.sampleSettings,
	}
	wr, err := b.CreateWriter(context.Background(), "some-bucket", "some/object/test.txt")
	assert.Equal(s.T(), nil, err)
	_, err = wr.Write([]byte("test data"))
	assert.Equal(s.T(), nil, err)
	err = wr.Close()
	assert.Equal(s.T(), nil, err)

	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/test.txt")
	it := s.client.Bucket("some-bucket").Objects(context.Background(), nil)
        for {
                attrs, err := it.Next()
                if err == iterator.Done {
                        break
                }
                if err != nil {
			fmt.Println("problem next", err)
                }
                fmt.Println(attrs.Name)
        }
	fmt.Println("objs in gcs err ", err)
	assert.Equal(s.T(), nil, err)
	assert.Equal(s.T(), []byte("test data"), res)
}

func (s *GcpTestSuite) TestUpload() {
	b := gcp.Backend{
		GcsClient: s.client,
		Settings:  s.sampleSettings,
	}
	err := b.UploadFile(context.Background(), []byte("test"), "some-bucket", "some/object/test.txt")
	assert.Equal(s.T(), nil, err)

	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/test.txt")
	it := s.client.Bucket("some-bucket").Objects(context.Background(), nil)
        for {
                attrs, err := it.Next()
                if err == iterator.Done {
                        break
                }
                if err != nil {
			fmt.Println("problem next", err)
                }
                fmt.Println(attrs.Name)
        }
	fmt.Println("objs in gcs err ", err)
	assert.Equal(s.T(), nil, err)
	assert.Equal(s.T(), []byte("test"), res)
}

func (s *GcpTestSuite) TestUploadNotValidLocation() {
	b := gcp.Backend{
		GcsClient: s.client,
		Settings:  s.sampleSettings,
	}
	err := b.UploadFile(context.Background(), []byte("test"), "nonexistent-bucket", "some/object/test.txt")
	assert.NotNil(s.T(), err)
}

func (s *GcpTestSuite) TestNewBackend() {
	res := gcp.NewBackend(s.sampleSettings, s.client)
	assert.NotNil(s.T(), res)
}

func TestGcpTestSuite(t *testing.T) {
	suite.Run(t, new(GcpTestSuite))
}

package gcp_test

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/cloudchacho/hedwig-firehose/gcp"

)

type GcpTestSuite struct {
	suite.Suite
	client *storage.Client
	server *fakestorage.Server
	sampleSettings gcp.FirehoseSettings
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
	s.sampleSettings = gcp.FirehoseSettings{
		MetadataBucket: "some-metadata-bucket",
		StagingBucket: "some-staging-bucket",
		OutputBucket: "some-output-bucket",
	}
}

func (s *GcpTestSuite) AfterTest(suiteName, testName string) {
	s.server.Stop()
}

func (s *GcpTestSuite) TestRead() {
	b := gcp.Backend{
		GcsClient: s.client,
		FirehoseSettings: s.sampleSettings,
	}
	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/file.txt")
	assert.Equal(s.T(), nil, err)
	assert.Equal(s.T(), []byte("inside the file"), res)
}

func (s *GcpTestSuite) TestUpload() {
	b := gcp.Backend{
		GcsClient: s.client,
		FirehoseSettings: s.sampleSettings,
	}
	err := b.UploadFile(context.Background(), []byte("test"), "some-bucket", "some/object/test.txt")
	assert.Equal(s.T(), nil, err)

	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/test.txt")
	assert.Equal(s.T(), nil, err)
	assert.Equal(s.T(), []byte("test"), res)
}

func TestGcpTestSuite(t *testing.T) {
	suite.Run(t, new(GcpTestSuite))
}




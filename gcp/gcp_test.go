package gcp_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/cloudchacho/hedwig-firehose"
	"github.com/cloudchacho/hedwig-firehose/gcp"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type GcpTestSuite struct {
	suite.Suite
	client *storage.Client
	server *fakestorage.Server
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
}

func (s *GcpTestSuite) AfterTest(suiteName, testName string) {
	s.server.Stop()
}

func (s *GcpTestSuite) TestRead() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/file.txt")
	s.Require().NoError(err)
	assert.Equal(s.T(), []byte("inside the file"), res)
}

func (s *GcpTestSuite) TestReadNotValidLocation() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/notthere.txt")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), res)
}

func (s *GcpTestSuite) TestUpload() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	err := b.UploadFile(context.Background(), []byte("test"), "some-bucket", "some/object/test.txt")
	assert.Equal(s.T(), nil, err)

	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/test.txt")
	s.Require().NoError(err)
	assert.Equal(s.T(), []byte("test"), res)
}

func (s *GcpTestSuite) TestUploadWriter() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	wr, err := b.CreateWriter(context.Background(), "some-bucket", "some/object/test.txt")
	assert.Equal(s.T(), nil, err)
	_, err = wr.Write([]byte("test data"))
	s.Require().NoError(err)
	err = wr.Close()
	s.Require().NoError(err)

	res, err := b.ReadFile(context.Background(), "some-bucket", "some/object/test.txt")
	assert.Equal(s.T(), nil, err)
	assert.Equal(s.T(), []byte("test data"), res)
}

func (s *GcpTestSuite) TestUploadNotValidLocation() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	err := b.UploadFile(context.Background(), []byte("test"), "nonexistent-bucket", "some/object/test.txt")
	assert.NotNil(s.T(), err)
}

func (s *GcpTestSuite) TestListFilesPrefixErr() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	_, err := b.ListFilesPrefix(context.Background(), "nonexistent-bucket", "!@#%$ ")
	assert.NotNil(s.T(), err)
}

func (s *GcpTestSuite) TestListFilesPrefix() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-bucket",
				Name:       "some/object/file1.txt",
			},
			Content: []byte("inside the file"),
		},
	)
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-bucket",
				Name:       "some/object/file2.txt",
			},
			Content: []byte("inside the file"),
		},
	)
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-bucket",
				Name:       "someother/object/file2.txt",
			},
			Content: []byte("inside the file"),
		},
	)
	fileNames, err := b.ListFilesPrefix(context.Background(), "some-bucket", "some/object")
	assert.Nil(s.T(), err)
	found := map[string]int{
		"some/object/file.txt":  0,
		"some/object/file1.txt": 0,
		"some/object/file2.txt": 0,
	}
	expected := map[string]int{
		"some/object/file.txt":  1,
		"some/object/file1.txt": 1,
		"some/object/file2.txt": 1,
	}
	for _, fileName := range fileNames {
		found[fileName]++
	}
	assert.Equal(s.T(), found, expected)
}

func (s *GcpTestSuite) TestDeleteFile() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	ctx := context.Background()
	err := b.DeleteFile(ctx, "some-bucket", "some/object/file.txt")
	assert.Nil(s.T(), err)
	_, err = s.client.Bucket("some-bucket").Object("some/object/file.txt").Attrs(ctx)
	assert.Equal(s.T(), err, storage.ErrObjectNotExist)
}

func (s *GcpTestSuite) TestDeleteFileErr() {
	b := gcp.Backend{
		GcsClient: s.client,
	}
	ctx := context.Background()
	err := b.DeleteFile(ctx, "some-bucket", "some/object/doesnotexist.txt")
	assert.NotNil(s.T(), err)
}

func (s *GcpTestSuite) TestGetNodeDeploymentId() {
	ctx := context.Background()
	b := gcp.Backend{
		GcsClient: s.client,
	}

	assert.Equal(s.T(), "", b.GetNodeId(ctx))
	assert.Equal(s.T(), "", b.GetDeploymentId(ctx))

	instance := "instance_1"
	deployment := "deployment_1"
	os.Setenv("GAE_INSTANCE", instance)
	os.Setenv("GAE_DEPLOYMENT_ID", deployment)

	assert.Equal(s.T(), instance, b.GetNodeId(ctx))
	assert.Equal(s.T(), deployment, b.GetDeploymentId(ctx))
}

func (s *GcpTestSuite) TestWriteLeaderFileErr() {
	ctx := context.Background()
	b := gcp.Backend{
		GcsClient: s.client,
	}
	instance := "instance_1"
	deployment := "deployment_1"

	jsonStr, _ := json.Marshal(firehose.LeaderFile{
		Timestamp:    "1659042621",
		DeploymentId: deployment,
		NodeId:       instance,
	})
	err := b.WriteLeaderFile(ctx, "nonexistent-bucket", jsonStr)
	assert.NotNil(s.T(), err)
}

func (s *GcpTestSuite) TestWriteLeaderFile() {
	ctx := context.Background()
	b := gcp.Backend{
		GcsClient: s.client,
	}
	instance := "instance_1"
	deployment := "deployment_1"

	jsonStr, _ := json.Marshal(firehose.LeaderFile{
		Timestamp:    "1659042621",
		DeploymentId: deployment,
		NodeId:       instance,
	})
	err := b.WriteLeaderFile(ctx, "some-bucket", jsonStr)
	assert.Nil(s.T(), err)

	res, err := b.ReadFile(ctx, "some-bucket", "leader.json")
	assert.Nil(s.T(), err)
	var result map[string]interface{}
	err = json.Unmarshal(res, &result)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), result["DeploymentId"], deployment)
	assert.Equal(s.T(), result["NodeId"], instance)
}

func (s *GcpTestSuite) TestWriteLeaderFileAlreadyExists() {
	ctx := context.Background()
	b := gcp.Backend{
		GcsClient: s.client,
	}
	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "some-bucket",
				Name:       "leader.json",
			},
			Content: []byte("inside the file"),
		},
	)

	instance := "instance_1"
	deployment := "deployment_1"
	jsonStr, _ := json.Marshal(firehose.LeaderFile{
		Timestamp:    "1659042621",
		DeploymentId: instance,
		NodeId:       deployment,
	})

	err := b.WriteLeaderFile(ctx, "some-bucket", jsonStr)
	assert.NotNil(s.T(), err)

}

func (s *GcpTestSuite) TestNewBackend() {
	res := gcp.NewBackend(s.client)
	assert.NotNil(s.T(), res)
}

func TestGcpTestSuite(t *testing.T) {
	suite.Run(t, new(GcpTestSuite))
}

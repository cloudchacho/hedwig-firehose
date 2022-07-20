package gcp

import (
	"os"
	"errors"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"
	"encoding/json"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/googleapi"
	"github.com/cloudchacho/hedwig-firehose/shared"
)

// This should satify interface for FirehoseBackend
type Backend struct {
	GcsClient *storage.Client
}

func (b *Backend) CreateWriter(ctx context.Context, uploadBucket string, uploadLocation string) (io.WriteCloser, error) {
	wc := b.GcsClient.Bucket(uploadBucket).Object(uploadLocation).NewWriter(ctx)
	return wc, nil
}

func (b *Backend) CreateReader(ctx context.Context, uploadBucket string, uploadLocation string) (io.ReadCloser, error) {
	return b.GcsClient.Bucket(uploadBucket).Object(uploadLocation).NewReader(ctx)
}

func (b *Backend) ListFilesPrefix(ctx context.Context, bucket string, prefix string) ([]string, error) {
	fileNames := []string{}
	it := b.GcsClient.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fileNames, fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}
		fileNames = append(fileNames, attrs.Name)
	}
	return fileNames, nil
}

func (b *Backend) DeleteFile(ctx context.Context, bucket string, location string) error {
	o := b.GcsClient.Bucket(bucket).Object(location)

	if err := o.Delete(ctx); err != nil {
		return fmt.Errorf("Object(%q).Delete: %v", location, err)
	}
	return nil
}

func (b *Backend) UploadFile(ctx context.Context, data []byte, uploadBucket string, uploadLocation string) error {
	buf := bytes.NewBuffer(data)

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	wc := b.GcsClient.Bucket(uploadBucket).Object(uploadLocation).NewWriter(ctx)
	wc.ChunkSize = 0

	if _, err := io.Copy(wc, buf); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}

	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}

	return nil
}

func (b *Backend) ReadFile(ctx context.Context, readBucket string, readLocation string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := b.GcsClient.Bucket(readBucket).Object(readLocation).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", readLocation, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	return data, nil
}

func (b *Backend) GetNodeId(ctx context.Context) string {
	return os.Getenv("GAE_INSTANCE")
}

func (b *Backend) GetDeploymentId(ctx context.Context) string {
	return os.Getenv("GAE_DEPLOYMENT_ID")
}

func (b *Backend) WriteLeaderFile(ctx context.Context, metadataBucket string, nodeId string, deploymentId string) error {
	w := b.GcsClient.Bucket(metadataBucket).Object("leader.json").If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	jsonStr, err := json.Marshal(map[string]string{
		"timestamp": fmt.Sprint(time.Now().Unix()),
		"deploymentId": deploymentId,
		"nodeId": nodeId,
	})
	if err != nil {
		return err
	}
	_, err = w.Write(jsonStr)
	if err != nil {
		return err
	}
	err = w.Close()
	var e *googleapi.Error
	if ok := errors.As(err, &e); ok {
		if e.Code == 412 {
			return shared.LeaderFileExists{}
		}
		return err
	}
	return nil
}

// NewBackend creates a Firehose on GCP
// The provider metadata produced by this Backend will have concrete type: gcp.Metadata
func NewBackend(client *storage.Client) *Backend {
	b := &Backend{
		client,
	}
	return b
}

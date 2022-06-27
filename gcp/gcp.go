package gcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
)

// This should satify interface for FirehoseBackend
type Backend struct {
	GcsClient *storage.Client
	Settings  Settings
}

// Settings for Hedwig firehose
type Settings struct {
	// bucket where leader file is saved
	MetadataBucket string

	// bucket where follower put intermediate files to be moved by leader
	StagingBucket string

	// final bucket for firehose files
	OutputBucket string
}

func (b *Backend) CreateWriter(ctx context.Context, uploadBucket string, uploadLocation string) (io.WriteCloser, error) {
	wc := b.GcsClient.Bucket(uploadBucket).Object(uploadLocation).NewWriter(ctx)
	return wc, nil
}

func (b *Backend) CreateReader(ctx context.Context, uploadBucket string, uploadLocation string) (io.ReadCloser, error) {
	return b.GcsClient.Bucket(uploadBucket).Object(uploadLocation).NewReader(ctx)
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

// NewBackend creates a Firehose on GCP
// The provider metadata produced by this Backend will have concrete type: gcp.Metadata
func NewBackend(settings Settings, client *storage.Client) *Backend {
	b := &Backend{
		client,
		settings,
	}
	return b
}

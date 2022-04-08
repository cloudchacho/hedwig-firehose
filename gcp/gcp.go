package gcp

import (
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

// This should satify interface for FirehoseBackend
type Backend struct {
	client    *pubsub.Client
	settings  Settings
}


// SubscriptionProject represents a tuple of subscription name and project for cross-project Google subscriptions
type SubscriptionProject struct {
	// Subscription name
	Subscription string

	// ProjectID
	ProjectID string
}

// Settings for Hedwig firehose
type Settings struct {
	// bucket where leader file is saved
	MetadataBucket string

	// bucket where follower put intermediate files to be moved by leader
	StagingBucket string

	// final bucket for firehose files
	OutputBucket string

	// interval when leader moves files to final bucket
	ScrapeInterval int

	// interval when follower flushes to staging bucket
	FlushAfter int

	// Firehose queue name, for requeueing
	QueueName string

	// GoogleCloudProject ID that contains Pub/Sub resources.
	GoogleCloudProject string

	// PubsubClientOptions is a list of options to pass to pubsub.NewClient. This may be useful to customize GRPC
	// behavior for example.
	PubsubClientOptions []option.ClientOption

	// FirehoseSubscriptions is a list of tuples of topic name and GCP project for project topic messages.
	// Google only.
	FirehoseSubscriptions []SubscriptionProject
}

func (b *Backend) initDefaults() {
	if b.settings.PubsubClientOptions == nil {
		b.settings.PubsubClientOptions = []option.ClientOption{}
	}
	if b.getLogger == nil {
		stdLogger := &hedwig.StdLogger{}
		b.getLogger = func(_ context.Context) hedwig.Logger { return stdLogger }
	}
}

// NewBackend creates a Firehose on GCP
// The provider metadata produced by this Backend will have concrete type: gcp.Metadata
func NewBackend(settings Settings, getLogger hedwig.GetLoggerFunc) *Backend {
	b := &Backend{settings: settings, getLogger: getLogger}
	b.initDefaults()
	return b
}

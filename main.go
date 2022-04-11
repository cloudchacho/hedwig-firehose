package main

import (
	"fmt"
)

type ProcessSettings struct {
	// interval when leader moves files to final bucket
	ScrapeInterval int

	// interval when follower flushes to staging bucket
	FlushAfter int
}

// FirehoseBackend is used for consuming messages from a transport and read/write to storage
type FirehoseBackend interface {
}

type Firehose struct {
	processSettings ProcessSettings
	firehoseBackend FirehoseBackend
}

func (fp *Firehose) RunFollower() {
}

func (fp *Firehose) RunLeader() {
}

// RunFirehose starts a Firehose running in leader of follower mode
func (f *Firehose) RunFirehose() {
	// 1. on start up determine if leader or followerBackend
	// 2. if leader call RunFollower
	// 3. else follower call RunFollower
}

func NewFirehose(firehoseBackend FirehoseBackend, processSettings ProcessSettings) *Firehose {
	// If we want to inject config from env, pass in when creating new
	f := &Firehose{
		processSettings: processSettings,
		firehoseBackend: firehoseBackend,
	}
	return f
}

func main() {
	fmt.Println("Hello World")
}

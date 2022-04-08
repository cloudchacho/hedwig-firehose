package main

// FirehoseBackend is used for consuming messages from a transport and read/write to storage
type FirehoseBackend interface {
}

type FirehoseProcess struct {
	firehoseBackend FirehoseBackend
}

type Firehose struct {
	FirehoseProcess
}

func (fp *FirehoseProcess) RunFollower() {
}

func (fp *FirehoseProcess) RunLeader() {
}

// RunFirehose starts a Firehose running in leader of follower mode
func (f *Firehose) RunFirehose(ctx context.Context, request ListenRequest) error {
	// 1. on start up determine if leader or followerBackend
	// 2. if leader call RunFollower
	// 3. else follower call RunFollower
}

func NewFirehose(FirehoseBackend FirehoseBackend) *Firehose {
	f := &Firehose{
		FirehoseProcess: FirehoseProcess{
			firehoseBackend: FirehoseBackend,
		}}
	return f
}

package shared

type LeaderFileExists struct{}

func (e LeaderFileExists) Error() string {
	return "Leader file already exists"
}

package job

const (
	StatusPending string = "pending"
	StatusRunning string = "running"
	StatusSuccess string = "success"
	StatusFailed  string = "failed"
	Timeout       string = "timeout"
)

type Job struct {
	ID         string `json:"id"`
	JobType    string `json:"jobtype"`
	Payload    string `json:"payload"`
	Status     string `json:"status"`
	RetryTimes int    `json:"retryTimes"`
	MaxRetries int    `json:"maxRetries"`
	Error      string `json:"error,omitempty"`
}

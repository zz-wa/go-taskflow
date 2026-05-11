package model

const (
	Pending string = "pending"
	Running string = "running"
	Success string = "success"
	Failed  string = "failed"
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

var JobStore map[string]*Job
var JobQueue chan *Job

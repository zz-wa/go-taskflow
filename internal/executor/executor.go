package executor

import (
	"errors"
	"go-taskflow/internal/job"
)

type Executor interface {
	Execute(j *job.Job) error
}

type Default struct{}

func (Default) Execute(j *job.Job) error {
	switch j.Payload {
	case "success":
		return nil
	case "fail":
		return errors.New("execute failed")
	case "flaky":
		if j.RetryTimes < j.MaxRetries-1 {
			return errors.New("job retry failed")
		}
		return nil
	}
	return nil
}

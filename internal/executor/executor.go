package executor

import (
	"context"
	"errors"
	"go-taskflow/internal/job"
	"time"
)

type Executor interface {
	Execute(ctx context.Context, j *job.Job) error
}

type Default struct{}

func (Default) Execute(ctx context.Context, j *job.Job) error {

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
	case "timeout":
		select {
		case <-time.After(5 * time.Second):
			return nil

		case <-ctx.Done():
			return ctx.Err()
		}

	}
	return nil
}

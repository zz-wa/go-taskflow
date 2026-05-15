package worker

import (
	"go-taskflow/internal/executor"
	"go-taskflow/internal/job"
	"testing"
	"time"
)

func TestPoolExecuteJobsWithRetry(t *testing.T) {
	tests := []struct {
		name           string
		payload        string
		wantStatus     string
		wantRetryTimes int
		wantError      bool
	}{
		{
			name:           "success job should finish without retry",
			payload:        "success",
			wantStatus:     job.StatusSuccess,
			wantRetryTimes: 0,
			wantError:      false,
		},
		{
			name:           "fail job should retry until failed",
			payload:        "fail",
			wantStatus:     job.StatusFailed,
			wantRetryTimes: 3,
			wantError:      true,
		},
		{
			name:           "flaky job should succeed after retries",
			payload:        "flaky",
			wantStatus:     job.StatusSuccess,
			wantRetryTimes: 2,
			wantError:      true,
		},
		{
			name:           "timeout job",
			payload:        "timeout",
			wantStatus:     job.StatusFailed,
			wantRetryTimes: 3,
			wantError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := job.NewMemStore()

			pool := New(
				Config{
					Workers:    3,
					QueueSize:  100,
					MaxRetries: 3,
					JobTimeout: 500 * time.Millisecond,
				},
				executor.Default{},
				store,
			)

			pool.Start()

			id := pool.Submit("test", tt.payload)

			pool.Shutdown()

			got, ok := store.Get(id)
			if !ok {
				t.Fatalf("job not found: id=%s", id)
			}

			if got.Status != tt.wantStatus {
				t.Fatalf("status = %s, want %s", got.Status, tt.wantStatus)
			}

			if got.RetryTimes != tt.wantRetryTimes {
				t.Fatalf("retryTimes = %d, want %d", got.RetryTimes, tt.wantRetryTimes)
			}

			if tt.wantError && got.Error == "" {
				t.Fatalf("error is empty, want non-empty error")
			}

			if !tt.wantError && got.Error != "" {
				t.Fatalf("error = %q, want empty", got.Error)
			}
		})
	}
}

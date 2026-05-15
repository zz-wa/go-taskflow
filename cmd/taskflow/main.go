package main

import (
	"fmt"
	"go-taskflow/internal/executor"
	"go-taskflow/internal/job"
	"go-taskflow/internal/worker"
	"time"
)

func main() {
	store := job.NewMemStore()

	pool := worker.New(worker.Config{Workers: 3, QueueSize: 100, MaxRetries: 3, JobTimeout: 500 * time.Millisecond}, executor.Default{}, store)

	pool.Start()

	ids := []string{
		pool.Submit("1", "success"),
		pool.Submit("1", "fail"),
		pool.Submit("1", "flaky"),
	}
	pool.Shutdown()

	for _, id := range ids {
		fmt.Println("job Status", store.GetStatus(id))
	}

}

package worker

import (
	"fmt"
	"go-taskflow/internal/executor"
	"go-taskflow/internal/job"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	Workers    int
	QueueSize  int
	MaxRetries int
}

type Pool struct {
	cfg      Config
	store    job.Store
	exec     executor.Executor
	queue    chan *job.Job
	workerWg sync.WaitGroup
	jobWg    sync.WaitGroup
}

func New(cfg Config, exec executor.Executor, store job.Store) *Pool {
	return &Pool{
		cfg:   cfg,
		store: store,
		exec:  exec,
		queue: make(chan *job.Job, cfg.QueueSize),
	}
}

func (p *Pool) Start() {
	for i := 0; i < p.cfg.Workers; i++ {
		p.workerWg.Add(1)
		go p.runWorker(i)
	}
}

func (p *Pool) Submit(jobType, payload string) string {
	j := &job.Job{
		ID:         uuid.New().String(),
		JobType:    jobType,
		Payload:    payload,
		Status:     job.StatusPending,
		MaxRetries: p.cfg.MaxRetries,
	}
	p.store.Put(j)
	p.jobWg.Add(1)
	p.queue <- j

	return j.ID
}

func (p *Pool) Shutdown() {
	p.jobWg.Wait()
	close(p.queue)
	p.workerWg.Wait()
}

func (p *Pool) runWorker(id int) {
	defer p.workerWg.Done()
	for j := range p.queue {
		fmt.Printf("worker - %d begin job %s\n", id, j.ID)
		p.store.UpdateStatus(j.ID, job.StatusRunning)

		time.Sleep(1 * time.Second)

		if err := p.exec.Execute(j); err != nil {
			p.HandleFail(j, err)
			continue
		}
		p.store.UpdateStatus(j.ID, job.StatusSuccess)
		p.jobWg.Done()
		fmt.Printf("worker - %d success job %s\n", id, j.ID)
	}
}

func (p *Pool) HandleFail(j *job.Job, err error) {
	j.RetryTimes++
	j.Error = err.Error()

	retry, ok := p.store.RecordFailed(j.ID, err)
	if !ok {
		return
	}
	if retry {
		p.queue <- j
	}
	p.jobWg.Done()
}

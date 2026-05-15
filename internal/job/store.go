package job

import (
	"sync"
)

type Store interface {
	Put(j *Job)
	UpdateStatus(id, status string)
	RecordFailed(id string, err error) (ShouldRetry bool, ok bool)
	GetStatus(id string) string
	Get(id string) (Job, bool)
}

type MemStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func NewMemStore() *MemStore {
	return &MemStore{
		jobs: make(map[string]*Job),
	}
}

func (s *MemStore) Put(j *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
}

func (s *MemStore) UpdateStatus(id, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok {
		j.Status = status
	}
}

func (s *MemStore) GetStatus(id string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if j, ok := s.jobs[id]; ok {
		return j.Status
	}

	return ""
}

func (s *MemStore) Get(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id]
	if !ok || j == nil {
		return Job{}, false
	}

	return *j, true
}

func (s *MemStore) RecordFailed(id string, err error) (ShouldRetry bool, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return false, false
	}

	j.RetryTimes++
	j.Error = err.Error()

	if j.RetryTimes < j.MaxRetries {
		j.Status = StatusPending
		return true, true
	}
	j.Status = StatusFailed

	return false, true
}

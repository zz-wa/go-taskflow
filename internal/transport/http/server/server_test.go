package server

import (
	"go-taskflow/internal/executor"
	"go-taskflow/internal/job"
	"go-taskflow/internal/worker"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealth(t *testing.T) {
	store := job.NewMemStore()
	pool := worker.New(worker.Config{
		Workers:    1,
		QueueSize:  10,
		MaxRetries: 3,
		JobTimeout: 500 * time.Millisecond,
	}, executor.Default{}, store)

	s := NewServer(store, pool)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rec.Code, http.StatusOK)
	}

}

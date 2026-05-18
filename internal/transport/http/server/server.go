package server

import (
	"encoding/json"
	"go-taskflow/internal/job"
	"go-taskflow/internal/worker"
	"net/http"
	"strings"
)

type Server struct {
	Store job.Store
	Pool  *worker.Pool
}

func NewServer(store job.Store, pool *worker.Pool) *Server {
	return &Server{
		Store: store,
		Pool:  pool,
	}
}

func (s *Server) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJson(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

type SubmitReq struct {
	JobType string `json:"jobtype"`
	Payload string `json:"payload"`
}

type SubmitRes struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (s *Server) SubmitHandle(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req SubmitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "failed to decode submit request")
		return
	}
	if req.JobType == "" || req.Payload == "" {
		writeError(w, http.StatusBadRequest, "jobtype and payload are required")
		return
	}
	id, err := s.Pool.Submit(req.JobType, req.Payload)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "queue is full")
		return
	}
	writeJson(w, http.StatusAccepted, SubmitRes{
		ID:     id,
		Status: job.StatusPending,
	})
}

func (s *Server) GetHandle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "jobid is required")
		return
	}

	j, ok := s.Store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJson(w, http.StatusOK, j)
}

func writeJson(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJson(w, status, map[string]string{
		"error": msg,
	})
}

package server

import "net/http"

func (s *Server) Handler() http.Handler {

	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.Health)
	mux.HandleFunc("/submit", s.SubmitHandle)
	mux.HandleFunc("/jobs/", s.GetHandle)
	return mux

}

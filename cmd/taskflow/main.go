package main

import (
	"context"
	"go-taskflow/internal/executor"
	"go-taskflow/internal/job"
	srv "go-taskflow/internal/transport/http/server"
	"go-taskflow/internal/worker"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	store := job.NewMemStore()

	pool := worker.New(worker.Config{Workers: 3, QueueSize: 100, MaxRetries: 3, JobTimeout: 500 * time.Millisecond}, executor.Default{}, store)

	pool.Start()

	server := srv.NewServer(store, pool)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: server.Handler(),
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
	pool.Shutdown()
}

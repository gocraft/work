package webui

import (
	"encoding/json"
	"fmt"
	"github.com/braintree/manners"
	"github.com/garyburd/redigo/redis"
	"github.com/gocraft/work"
	"net/http"
	"sync"
)

type WebUIServer struct {
	namespace string
	pool      *redis.Pool
	client    *work.Client
	hostPort  string
	server    *manners.GracefulServer
	wg        sync.WaitGroup
}

// TODO: rename to NewServer
func NewWebUIServer(namespace string, pool *redis.Pool, hostPort string) *WebUIServer {
	return &WebUIServer{
		namespace: namespace,
		pool:      pool,
		client:    work.NewClient(namespace, pool),
		hostPort:  hostPort,
		server:    manners.NewServer(),
	}
}

func (w *WebUIServer) Start() {
	w.wg.Add(1)
	go func(w *WebUIServer) {
		w.server.ListenAndServe(w.hostPort, w)
		w.wg.Done()
	}(w)
}

func (w *WebUIServer) Stop() {
	w.server.Shutdown <- true
	w.wg.Wait()
}

func (s *WebUIServer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	if r.URL.Path == "/queues" {
		response, err := s.client.Queues()
		if err != nil {
			renderError(rw, err)
			return
		}

		jsonData, err := json.MarshalIndent(response, "", "\t")
		if err != nil {
			renderError(rw, err)
			return
		}
		rw.Write(jsonData)
	} else if r.URL.Path == "/worker_pools" {
		response, err := s.workerPoolStatuses()
		if err != nil {
			renderError(rw, err)
			return
		}

		jsonData, err := json.MarshalIndent(response, "", "\t")
		if err != nil {
			renderError(rw, err)
			return
		}
		rw.Write(jsonData)
	} else if r.URL.Path == "/busy_workers" {
		response, err := s.busyWorkerStatuses()
		if err != nil {
			renderError(rw, err)
			return
		}

		jsonData, err := json.MarshalIndent(response, "", "\t")
		if err != nil {
			renderError(rw, err)
			return
		}
		rw.Write(jsonData)
	} else if r.URL.Path == "/retry_jobs" {
	} else if r.URL.Path == "/scheduled_jobs" {
	} else if r.URL.Path == "/dead_jobs" {
	} else {
		renderNotFound(rw)
	}
}

func renderNotFound(rw http.ResponseWriter) {
	rw.WriteHeader(404)
	fmt.Fprintf(rw, `{"error": "not_found"}`)
}

func renderError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(500)
	fmt.Fprintf(rw, `{"error": "%s"}`, err.Error())
}

func (s *WebUIServer) workerPoolStatuses() ([]*work.WorkerPoolStatus, error) {
	workerPoolIDs, err := s.client.WorkerPoolIDs()
	if err != nil {
		return nil, err
	}

	return s.client.WorkerPoolStatuses(workerPoolIDs)
}

func (s *WebUIServer) busyWorkerStatuses() ([]*work.WorkerStatus, error) {
	poolStatuses, err := s.workerPoolStatuses()
	if err != nil {
		return nil, err
	}

	var workerIDs []string

	for _, ps := range poolStatuses {
		workerIDs = append(workerIDs, ps.WorkerIDs...)
	}

	statuses, err := s.client.WorkerStatuses(workerIDs)
	if err != nil {
		return nil, err
	}

	var busyStatuses []*work.WorkerStatus
	for _, status := range statuses {
		if status.IsBusy {
			busyStatuses = append(busyStatuses, status)
		}
	}
	return busyStatuses, nil
}

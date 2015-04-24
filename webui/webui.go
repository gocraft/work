package webui

import (
	"encoding/json"
	"fmt"
	"github.com/braintree/manners"
	"github.com/garyburd/redigo/redis"
	"github.com/gocraft/work"
	"net/http"
	"strconv"
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
		response, err := s.client.WorkerPoolHeartbeats()
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
		response, err := s.busyWorkerObservations()
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
		page, err := parsePage(r)
		if err != nil {
			renderError(rw, err)
			return
		}

		jobs, count, err := s.client.RetryJobs(page)
		if err != nil {
			renderError(rw, err)
			return
		}

		response := struct {
			Count int64
			Jobs  []*work.RetryJob
		}{Count: count, Jobs: jobs}

		jsonData, err := json.MarshalIndent(response, "", "\t")
		if err != nil {
			renderError(rw, err)
			return
		}
		rw.Write(jsonData)
	} else if r.URL.Path == "/scheduled_jobs" {
		page, err := parsePage(r)
		if err != nil {
			renderError(rw, err)
			return
		}

		jobs, count, err := s.client.ScheduledJobs(page)
		if err != nil {
			renderError(rw, err)
			return
		}

		response := struct {
			Count int64
			Jobs  []*work.ScheduledJob
		}{Count: count, Jobs: jobs}

		jsonData, err := json.MarshalIndent(response, "", "\t")
		if err != nil {
			renderError(rw, err)
			return
		}
		rw.Write(jsonData)
	} else if r.URL.Path == "/dead_jobs" {
		page, err := parsePage(r)
		if err != nil {
			renderError(rw, err)
			return
		}

		jobs, count, err := s.client.DeadJobs(page)
		if err != nil {
			renderError(rw, err)
			return
		}

		response := struct {
			Count int64
			Jobs  []*work.DeadJob
		}{Count: count, Jobs: jobs}

		jsonData, err := json.MarshalIndent(response, "", "\t")
		if err != nil {
			renderError(rw, err)
			return
		}
		rw.Write(jsonData)
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

func parsePage(r *http.Request) (uint, error) {
	err := r.ParseForm()
	if err != nil {
		return 0, err
	}

	pageStr := r.Form.Get("page")
	if pageStr == "" {
		pageStr = "1"
	}

	page, err := strconv.ParseUint(pageStr, 10, 0)
	return uint(page), err
}

func (s *WebUIServer) busyWorkerObservations() ([]*work.WorkerObservation, error) {
	observations, err := s.client.WorkerObservations()
	if err != nil {
		return nil, err
	}

	var busyObservations []*work.WorkerObservation
	for _, ob := range observations {
		if ob.IsBusy {
			busyObservations = append(busyObservations, ob)
		}
	}
	return busyObservations, nil
}

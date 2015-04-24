package webui

import (
	"encoding/json"
	"fmt"
	"github.com/braintree/manners"
	"github.com/garyburd/redigo/redis"
	"github.com/gocraft/web"
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
	router    *web.Router
}

type context struct {
	*WebUIServer
}

func NewServer(namespace string, pool *redis.Pool, hostPort string) *WebUIServer {
	router := web.New(context{})
	server := &WebUIServer{
		namespace: namespace,
		pool:      pool,
		client:    work.NewClient(namespace, pool),
		hostPort:  hostPort,
		server:    manners.NewServer(),
		router:    router,
	}

	router.Middleware(func(c *context, rw web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		c.WebUIServer = server
		next(rw, r)
	})
	router.Middleware(func(rw web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		rw.Header().Set("Content-Type", "application/json; charset=utf-8")
		next(rw, r)
	})
	router.Get("/queues", (*context).queues)
	router.Get("/worker_pools", (*context).workerPools)
	router.Get("/busy_workers", (*context).busyWorkers)
	router.Get("/retry_jobs", (*context).retryJobs)
	router.Get("/scheduled_jobs", (*context).scheduledJobs)
	router.Get("/dead_jobs", (*context).deadJobs)

	return server
}

func (w *WebUIServer) Start() {
	w.wg.Add(1)
	go func(w *WebUIServer) {
		w.server.ListenAndServe(w.hostPort, w.router)
		w.wg.Done()
	}(w)
}

func (w *WebUIServer) Stop() {
	w.server.Shutdown <- true
	w.wg.Wait()
}

func (c *context) queues(rw web.ResponseWriter, r *web.Request) {
	response, err := c.client.Queues()
	render(rw, response, err)
}

func (c *context) workerPools(rw web.ResponseWriter, r *web.Request) {
	response, err := c.client.WorkerPoolHeartbeats()
	render(rw, response, err)
}

func (c *context) busyWorkers(rw web.ResponseWriter, r *web.Request) {
	observations, err := c.client.WorkerObservations()
	if err != nil {
		renderError(rw, err)
		return
	}

	var busyObservations []*work.WorkerObservation
	for _, ob := range observations {
		if ob.IsBusy {
			busyObservations = append(busyObservations, ob)
		}
	}

	render(rw, busyObservations, err)
}

func (c *context) retryJobs(rw web.ResponseWriter, r *web.Request) {
	page, err := parsePage(r)
	if err != nil {
		renderError(rw, err)
		return
	}

	jobs, count, err := c.client.RetryJobs(page)
	if err != nil {
		renderError(rw, err)
		return
	}

	response := struct {
		Count int64
		Jobs  []*work.RetryJob
	}{Count: count, Jobs: jobs}

	render(rw, response, err)
}

func (c *context) scheduledJobs(rw web.ResponseWriter, r *web.Request) {
	page, err := parsePage(r)
	if err != nil {
		renderError(rw, err)
		return
	}

	jobs, count, err := c.client.ScheduledJobs(page)
	if err != nil {
		renderError(rw, err)
		return
	}

	response := struct {
		Count int64
		Jobs  []*work.ScheduledJob
	}{Count: count, Jobs: jobs}

	render(rw, response, err)
}

func (c *context) deadJobs(rw web.ResponseWriter, r *web.Request) {
	page, err := parsePage(r)
	if err != nil {
		renderError(rw, err)
		return
	}

	jobs, count, err := c.client.DeadJobs(page)
	if err != nil {
		renderError(rw, err)
		return
	}

	response := struct {
		Count int64
		Jobs  []*work.DeadJob
	}{Count: count, Jobs: jobs}
	render(rw, response, err)
}

func render(rw web.ResponseWriter, jsonable interface{}, err error) {
	if err != nil {
		renderError(rw, err)
		return
	}

	jsonData, err := json.MarshalIndent(jsonable, "", "\t")
	if err != nil {
		renderError(rw, err)
		return
	}
	rw.Write(jsonData)
}

func renderError(rw http.ResponseWriter, err error) {
	rw.WriteHeader(500)
	fmt.Fprintf(rw, `{"error": "%s"}`, err.Error())
}

func parsePage(r *web.Request) (uint, error) {
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

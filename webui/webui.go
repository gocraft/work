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
		server:    manners.NewWithServer(&http.Server{Addr: hostPort, Handler: router}),
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
	router.Post("/delete_dead_job/:died_at:\\d.*/:job_id", (*context).deleteDeadJob)
	router.Post("/retry_dead_job/:died_at:\\d.*/:job_id", (*context).retryDeadJob)
	router.Post("/delete_all_dead_jobs", (*context).deleteAllDeadJobs)
	router.Post("/retry_all_dead_jobs", (*context).retryAllDeadJobs)

	return server
}

func (w *WebUIServer) Start() {
	w.wg.Add(1)
	go func(w *WebUIServer) {
		w.server.ListenAndServe()
		w.wg.Done()
	}(w)
}

func (w *WebUIServer) Stop() {
	w.server.Close()
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
		Count int64            `json:"count"`
		Jobs  []*work.RetryJob `json:"jobs"`
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
		Count int64                `json:"count"`
		Jobs  []*work.ScheduledJob `json:"jobs"`
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
		Count int64           `json:"count"`
		Jobs  []*work.DeadJob `json:"jobs"`
	}{Count: count, Jobs: jobs}

	render(rw, response, err)
}

func (c *context) deleteDeadJob(rw web.ResponseWriter, r *web.Request) {
	diedAt, err := strconv.ParseInt(r.PathParams["died_at"], 10, 64)
	if err != nil {
		renderError(rw, err)
		return
	}

	job := &work.DeadJob{
		DiedAt: diedAt,
		Job:    &work.Job{ID: r.PathParams["job_id"]},
	}

	err = c.client.DeleteDeadJob(job)

	render(rw, map[string]string{"status": "ok"}, err)
}

func (c *context) retryDeadJob(rw web.ResponseWriter, r *web.Request) {
	diedAt, err := strconv.ParseInt(r.PathParams["died_at"], 10, 64)
	if err != nil {
		renderError(rw, err)
		return
	}

	job := &work.DeadJob{
		DiedAt: diedAt,
		Job:    &work.Job{ID: r.PathParams["job_id"]},
	}

	err = c.client.RetryDeadJob(job)

	render(rw, map[string]string{"status": "ok"}, err)
}

func (c *context) deleteAllDeadJobs(rw web.ResponseWriter, r *web.Request) {
	err := c.client.DeleteAllDeadJobs()
	render(rw, map[string]string{"status": "ok"}, err)
}

func (c *context) retryAllDeadJobs(rw web.ResponseWriter, r *web.Request) {
	err := c.client.RetryAllDeadJobs()
	render(rw, map[string]string{"status": "ok"}, err)
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

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
	if r.URL.Path == "/jobs" {
		response, err := s.client.JobStatuses()
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

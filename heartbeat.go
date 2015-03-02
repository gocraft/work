package work

import (
	// "fmt"
	"github.com/garyburd/redigo/redis"
	"os"
	"sort"
	"strings"
	"time"
)

type workerPoolHeartbeat struct {
	workerPoolID string
	namespace    string // eg, "myapp-work"
	pool         *redis.Pool

	//
	concurrency uint
	jobNames    string
	startedAt   int64
	pid         int
	hostname    string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newWorkerPoolHeartbeat(namespace string, pool *redis.Pool, workerPoolID string, jobTypes map[string]*jobType, concurrency uint) *workerPoolHeartbeat {
	h := &workerPoolHeartbeat{
		workerPoolID: workerPoolID,
		namespace:    namespace,
		pool:         pool,

		concurrency: concurrency,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}

	jobNames := make([]string, 0, len(jobTypes))
	for k, _ := range jobTypes {
		jobNames = append(jobNames, k)
	}
	sort.Strings(jobNames)
	h.jobNames = strings.Join(jobNames, ",")

	h.pid = os.Getpid()
	host, err := os.Hostname()
	if err != nil {
		logError("heartbeat.hostname", err)
		host = "hostname_errored"
	}
	h.hostname = host

	return h
}

func (h *workerPoolHeartbeat) start() {
	go h.loop()
}

func (h *workerPoolHeartbeat) stop() {
	close(h.stopChan)
	<-h.doneStoppingChan
}

func (h *workerPoolHeartbeat) loop() {
	h.startedAt = nowEpochSeconds()
	h.heartbeat() // do it right away
	ticker := time.Tick(5000 * time.Millisecond)
	for {
		select {
		case <-h.stopChan:
			h.removeHeartbeat()
			close(h.doneStoppingChan)
			return
		case <-ticker:
			h.heartbeat()
		}
	}
}

func (h *workerPoolHeartbeat) heartbeat() {
	conn := h.pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(h.namespace)
	heartbeatKey := redisKeyHeartbeat(h.namespace, h.workerPoolID)

	conn.Send("SADD", workerPoolsKey, h.workerPoolID)
	conn.Send("HMSET", heartbeatKey,
		"heartbeat_at", nowEpochSeconds(),
		"started_at", h.startedAt,
		"job_names", h.jobNames,
		"concurrency", h.concurrency,
		"host", h.hostname,
		"pid", h.pid,
	)
	conn.Send("EXPIRE", heartbeatKey, 60)

	if err := conn.Flush(); err != nil {
		logError("heartbeat", err)
	}
}

func (h *workerPoolHeartbeat) removeHeartbeat() {
	conn := h.pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(h.namespace)
	heartbeatKey := redisKeyHeartbeat(h.namespace, h.workerPoolID)

	conn.Send("SREM", workerPoolsKey, h.workerPoolID)
	conn.Send("DEL", heartbeatKey)

	if err := conn.Flush(); err != nil {
		logError("remove_heartbeat", err)
	}
}

package webui

import (
	"encoding/json"
	// "fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gocraft/work"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestWebUIStartStop(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	s := NewWebUIServer(ns, pool, ":6666")
	s.Start()
	s.Stop()
}

type TestContext struct{}

func TestWebUIJobs(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	// Get some stuff to to show up in the jobs:
	enqueuer := work.NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue("wat", 1, 2)
	assert.NoError(t, err)
	enqueuer.Enqueue("foo", 3, 4)
	enqueuer.Enqueue("zaz", 3, 4)

	// Start a pool to work on it. It's going to work on the queues
	// side effect of that is knowing which jobs are avail
	wp := work.NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Job("wat", func(job *work.Job) error {
		return nil
	})
	wp.Job("foo", func(job *work.Job) error {
		return nil
	})
	wp.Job("zaz", func(job *work.Job) error {
		return nil
	})
	wp.Start()
	time.Sleep(20 * time.Millisecond)
	wp.Stop()

	// Now that we have the jobs, populate some queues
	enqueuer.Enqueue("wat", 1, 2)
	enqueuer.Enqueue("wat", 1, 2)
	enqueuer.Enqueue("wat", 1, 2)
	enqueuer.Enqueue("foo", 3, 4)
	enqueuer.Enqueue("foo", 3, 4)
	enqueuer.Enqueue("zaz", 3, 4)

	s := NewWebUIServer(ns, pool, ":6666")

	recorder := httptest.NewRecorder()
	request, _ := http.NewRequest("GET", "/jobs", nil)
	s.ServeHTTP(recorder, request)
	assert.Equal(t, 200, recorder.Code)

	var res []interface{}
	err = json.Unmarshal(recorder.Body.Bytes(), &res)
	assert.NoError(t, err)

	assert.Equal(t, 3, len(res))

	foomap, ok := res[0].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "foo", foomap["JobName"])
	assert.Equal(t, 2, foomap["Count"])
	assert.Equal(t, 0, foomap["Latency"])
}

func TestWebUIWorkerPools(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	wp := work.NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Job("wat", func(job *work.Job) error { return nil })
	wp.Job("bob", func(job *work.Job) error { return nil })
	wp.Start()
	defer wp.Stop()

	wp2 := work.NewWorkerPool(TestContext{}, 11, ns, pool)
	wp2.Job("foo", func(job *work.Job) error { return nil })
	wp2.Job("bar", func(job *work.Job) error { return nil })
	wp2.Start()
	defer wp2.Stop()

	time.Sleep(20 * time.Millisecond)

	s := NewWebUIServer(ns, pool, ":6666")

	recorder := httptest.NewRecorder()
	request, _ := http.NewRequest("GET", "/worker_pools", nil)
	s.ServeHTTP(recorder, request)
	assert.Equal(t, 200, recorder.Code)

	var res []interface{}
	err := json.Unmarshal(recorder.Body.Bytes(), &res)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(res))

	w1stat, ok := res[0].(map[string]interface{})
	assert.True(t, ok)
	assert.True(t, w1stat["WorkerPoolID"] != "")
	// NOTE: WorkerPoolStatus is tested elsewhere.
}

func TestWebUIBusyWorkers(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	// Keep a job in the in-progress state without using sleeps
	wgroup := sync.WaitGroup{}
	wgroup2 := sync.WaitGroup{}
	wgroup2.Add(1)

	wp := work.NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Job("wat", func(job *work.Job) error {
		wgroup2.Done()
		wgroup.Wait()
		return nil
	})
	wp.Start()
	defer wp.Stop()

	wp2 := work.NewWorkerPool(TestContext{}, 11, ns, pool)
	wp2.Start()
	defer wp2.Stop()

	time.Sleep(10 * time.Millisecond)

	s := NewWebUIServer(ns, pool, ":6666")

	recorder := httptest.NewRecorder()
	request, _ := http.NewRequest("GET", "/busy_workers", nil)
	s.ServeHTTP(recorder, request)
	assert.Equal(t, 200, recorder.Code)

	var res []interface{}
	err := json.Unmarshal(recorder.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(res))

	wgroup.Add(1)

	// Ok, now let's make a busy worker
	enqueuer := work.NewEnqueuer(ns, pool)
	enqueuer.Enqueue("wat", 1, 2)
	wgroup2.Wait()

	recorder = httptest.NewRecorder()
	request, _ = http.NewRequest("GET", "/busy_workers", nil)
	s.ServeHTTP(recorder, request)
	wgroup.Done()
	assert.Equal(t, 200, recorder.Code)
	err = json.Unmarshal(recorder.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))

	if len(res) == 1 {
		hash, ok := res[0].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "wat", hash["JobName"])
		assert.Equal(t, true, hash["IsBusy"])
	}
}

func newTestPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   3,
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		},
		Wait: true,
	}
}

func cleanKeyspace(namespace string, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", namespace+"*"))
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if _, err := conn.Do("DEL", k); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}

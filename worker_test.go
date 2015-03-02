package work

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestWorkerBasics(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	job1 := "job1"
	job2 := "job2"
	job3 := "job3"

	deleteQueue(pool, ns, job1)
	deleteQueue(pool, ns, job2)
	deleteQueue(pool, ns, job3)
	deleteRetryAndDead(pool, ns)

	var arg1 float64
	var arg2 float64
	var arg3 float64

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg1 = job.Args[0].(float64)
			return nil
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg2 = job.Args[0].(float64)
			return nil
		},
	}
	jobTypes[job3] = &jobType{
		Name:       job3,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg3 = job.Args[0].(float64)
			return nil
		},
	}

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue(job1, 1)
	assert.Nil(t, err)
	err = enqueuer.Enqueue(job2, 2)
	assert.Nil(t, err)
	err = enqueuer.Enqueue(job3, 3)
	assert.Nil(t, err)

	w := newWorker(ns, pool, jobTypes)
	w.start()
	w.join()
	w.stop()

	// make sure the jobs ran (side effect of setting these variables to the job arguments)
	assert.Equal(t, 1.0, arg1)
	assert.Equal(t, 2.0, arg2)
	assert.Equal(t, 3.0, arg3)

	// nothing in retries or dead
	assert.Equal(t, 0, zsetSize(pool, redisKeyRetry(ns)))
	assert.Equal(t, 0, zsetSize(pool, redisKeyDead(ns)))

	// Nothing in the queues or in-progress queues
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job2)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job3)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job1)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job2)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job3)))
	
	// nothing in the worker status
	h := readHash(pool, redisKeyWorkerStatus(ns, w.workerID))
	assert.Equal(t, 0, len(h))
}

func TestWorkerInProgress(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	job1 := "job1"
	deleteQueue(pool, ns, job1)
	deleteRetryAndDead(pool, ns)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue(job1, 1)
	assert.Nil(t, err)

	w := newWorker(ns, pool, jobTypes)
	w.start()

	// instead of w.forceIter(), we'll wait for 10 milliseconds to let the job start
	// The job will then sleep for 30ms. In that time, we should be able to see something in the in-progress queue.
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.Equal(t, 1, listSize(pool, redisKeyJobsInProgress(ns, job1)))
	
	// nothing in the worker status
	w.observer.join()
	h := readHash(pool, redisKeyWorkerStatus(ns, w.workerID))
	assert.Equal(t, job1, h["job_name"])
	assert.Equal(t, `[1]`, h["args"])
	// NOTE: we could check for job_id and started_at, but it's a PITA and it's tested in observer_test.

	w.join()
	w.stop()

	// At this point, it should all be empty.
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job1)))
	
	// nothing in the worker status
	h = readHash(pool, redisKeyWorkerStatus(ns, w.workerID))
	assert.Equal(t, 0, len(h))
}

func TestWorkerRetry(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	job1 := "job1"
	deleteQueue(pool, ns, job1)
	deleteRetryAndDead(pool, ns)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 3},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			return fmt.Errorf("sorry kid")
		},
	}

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue(job1, 1)
	assert.Nil(t, err)
	w := newWorker(ns, pool, jobTypes)
	w.start()
	w.join()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.Equal(t, 1, zsetSize(pool, redisKeyRetry(ns)))
	assert.Equal(t, 0, zsetSize(pool, redisKeyDead(ns)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job1)))

	// Get the job on the retry queue
	ts, job := jobOnZset(pool, redisKeyRetry(ns))

	assert.True(t, ts > nowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (nowEpochSeconds()+80)) // but less than a minute from now (first failure)

	assert.Equal(t, job1, job.Name) // basics are preserved
	// todo: check that job.EnqueuedAt didn't change. Need mocking for that.
	assert.Equal(t, 1, job.Fails)
	assert.Equal(t, "sorry kid", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
}

func TestWorkerDead(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	job1 := "job1"
	job2 := "job2"
	deleteQueue(pool, ns, job1)
	deleteQueue(pool, ns, job2)
	deleteRetryAndDead(pool, ns)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 0},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			return fmt.Errorf("sorry kid1")
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1, MaxFails: 0, SkipDead: true},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			return fmt.Errorf("sorry kid2")
		},
	}

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue(job1, 1)
	assert.Nil(t, err)
	err = enqueuer.Enqueue(job2, 2)
	assert.Nil(t, err)
	w := newWorker(ns, pool, jobTypes)
	w.start()
	w.join()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.Equal(t, 0, zsetSize(pool, redisKeyRetry(ns)))
	assert.Equal(t, 1, zsetSize(pool, redisKeyDead(ns)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job1)))

	// Get the job on the retry queue
	ts, job := jobOnZset(pool, redisKeyDead(ns))

	assert.True(t, ts > nowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (nowEpochSeconds()+60)) // but less than a minute from now (first failure)

	assert.Equal(t, job1, job.Name) // basics are preserved
	// todo: check that job.EnqueuedAt didn't change. Need mocking for that.
	assert.Equal(t, 1, job.Fails)
	assert.Equal(t, "sorry kid1", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
}

func deleteQueue(pool *redis.Pool, namespace, jobName string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

func deleteRetryAndDead(pool *redis.Pool, namespace string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyRetry(namespace), redisKeyDead(namespace))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
}

func zsetSize(pool *redis.Pool, key string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("ZCARD", key))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
	return v
}

func listSize(pool *redis.Pool, key string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("LLEN", key))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
	return v
}

func jobOnZset(pool *redis.Pool, key string) (int64, *Job) {
	conn := pool.Get()
	defer conn.Close()

	v, err := conn.Do("ZRANGE", key, 0, 0, "WITHSCORES")
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}

	vv := v.([]interface{})

	job, err := newJob(vv[0].([]byte), nil, nil)
	if err != nil {
		panic("couldn't get job: " + err.Error())
	}

	score := vv[1].([]byte)
	scoreInt, err := strconv.ParseInt(string(score), 10, 64)
	if err != nil {
		panic("couldn't parse int: " + err.Error())
	}

	return scoreInt, job
}

package work

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

type tstCtx struct {
	a int
	bytes.Buffer
}

func (c *tstCtx) record(s string) {
	_, _ = c.WriteString(s)
}

var tstCtxType = reflect.TypeOf(tstCtx{})

func TestWorkerPoolHandlerValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *Job) error { return nil }, true},
		{func(c *tstCtx, j *Job) error { return nil }, true},
		{func(c *tstCtx, j *Job) {}, false},
		{func(c *tstCtx, j *Job) string { return "" }, false},
		{func(c *tstCtx, j *Job) (error, string) { return nil, "" }, false},
		{func(c *tstCtx) error { return nil }, false},
		{func(c tstCtx, j *Job) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *tstCtx, j *Job, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidHandlerType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolMiddlewareValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *Job, n NextMiddlewareFunc) error { return nil }, true},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) error { return nil }, true},
		{func(c *tstCtx, j *Job) error { return nil }, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) {}, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) string { return "" }, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) (error, string) { return nil, "" }, false},
		{func(c *tstCtx, n NextMiddlewareFunc) error { return nil }, false},
		{func(c tstCtx, j *Job, n NextMiddlewareFunc) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *tstCtx, j *Job, wat string) error { return nil }, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidMiddlewareType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolStartStop(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	wp := NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Start()
	wp.Start()
	wp.Stop()
	wp.Stop()
	wp.Start()
	wp.Stop()
}

func TestWorkerPoolValidations(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	wp := NewWorkerPool(TestContext{}, 10, ns, pool)

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your middleware function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using bad middleware")
			}
		}()

		wp.Middleware(TestWorkerPoolValidations)
	}()

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your handler function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using a bad handler")
			}
		}()

		wp.Job("wat", TestWorkerPoolValidations)
	}()
}

func TestWorkersPoolRunSingleThreaded(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	job1 := "job1"
	numJobs, concurrency, sleepTime := 5, 5, 1000
	wp := setupTestWorkerPool(pool, ns, job1, concurrency, JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := NewEnqueuer(ns, pool)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}
	jobsQueued := listSize(pool, redisKeyJobs(ns, job1))
	// make sure we've some jobs queued up
	assert.True(t, jobsQueued > 1, "should be %v jobs queued up, but only found %v", numJobs, jobsQueued)

	// now make sure the during the duration of job execution there is never > 1 job in flight
	// wp.Start()
	start := time.Now()
	totalRuntime := time.Duration(sleepTime * numJobs) * time.Millisecond
	for time.Since(start) < totalRuntime {
		jobsInProgress := listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1))
		assert.True(t, jobsInProgress <= 1, "jobsInProgress should never exceed 1: actual=%d", jobsInProgress)
		// lock count for the job and lock info for the pool should never exceed 1
		jobLockCount := getInt64(pool, redisKeyJobsLock(ns, job1))
		assert.True(t, jobLockCount <= 1, "global lock count for job should never exceed 1, got: %v", jobLockCount)
		wpLockCount := hgetInt64(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID)
		assert.True(t, wpLockCount <= 1, "lock count for the worker pool should never exceed 1: actual=%v", wpLockCount)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
}

func TestWorkerPoolPauseSingleThreadedJobs(t *testing.T) {
	pool := newTestPool(":6379")
	ns, job1 := "work", "job1"
	numJobs, concurrency, sleepTime := 5, 5, 2
	wp := setupTestWorkerPool(pool, ns, job1, concurrency, JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := NewEnqueuer(ns, pool)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}
	// allow time for a job to start
	time.Sleep(1 * time.Millisecond)
	// pause work and allow time for any outstanding jobs to finish
	pauseJobs(ns, job1, pool)
	time.Sleep(2 * time.Millisecond)
	// check that we still have some jobs to process
	assert.True(t, listSize(pool, redisKeyJobs(ns, job1)) > int64(0))

	// now make sure no jobs get started until we unpause
	start := time.Now()
	totalRuntime := time.Duration(sleepTime * numJobs) * time.Millisecond
	for time.Since(start) < totalRuntime {
		assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
		// lock count for the job and lock info for the pool should both be at 1 while job is running
		assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, job1)))
		assert.EqualValues(t, 0, hgetInt64(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}

	// unpause work and get past the backoff time
	unpauseJobs(ns, job1, pool)
	time.Sleep(10 * time.Millisecond)

	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
}

// Test Helpers
func (t *TestContext) SleepyJob(job *Job) error {
	sleepTime := time.Duration(job.ArgInt64("sleep"))
	time.Sleep(sleepTime * time.Millisecond)
	return nil
}

func setupTestWorkerPool(pool *redis.Pool, namespace, jobName string, concurrency int, jobOpts JobOptions) *WorkerPool {
	cleanKeyspace(namespace, pool)
	//deleteQueue(pool, namespace, jobName)
	//deleteRetryAndDead(pool, namespace)
	//deletePausedAndLockedKeys(namespace, jobName, pool)

	wp := NewWorkerPool(TestContext{}, uint(concurrency), namespace, pool)
	wp.JobWithOptions(jobName, jobOpts, (*TestContext).SleepyJob)
	// reset the backoff times to help with testing
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	return wp
}

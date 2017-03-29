package work

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
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

func (t *TestContext) SleepyJob(job *Job) error {
	sleepTime := time.Duration(job.ArgInt64("sleep"))
	time.Sleep(sleepTime * time.Millisecond)
	return nil
}

func TestWorkersPoolRunExclusiveJobs(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	job1 := "job1"
	numJobs, concurrency, sleepTime := 5, 5, 2
	deleteQueue(pool, ns, job1)
	deleteRetryAndDead(pool, ns)
	deletePausedKey(ns, job1, pool)

	wp := NewWorkerPool(TestContext{}, uint(concurrency), ns, pool)
	wp.JobWithOptions(job1, JobOptions{Priority: 1, RunExclusiveJobs: true}, (*TestContext).SleepyJob)
	wp.Start()

	// enqueue some jobs
	enqueuer := NewEnqueuer(ns, pool)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}
	assert.True(t, int64(numJobs) >= listSize(pool, redisKeyJobs(ns, job1)))

	// now make sure the during the duration of job execution there is never > 1 job in flight
	start := time.Now()
	totalRuntime := time.Duration(sleepTime * numJobs) * time.Millisecond
	for time.Since(start) < totalRuntime {
		jobsInProgress := listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1))
		assert.True(t, jobsInProgress <= 1)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
}

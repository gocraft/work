package work

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"github.com/jalkanen/work"
	"time"
	"sync/atomic"
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

type emptyCtx struct{}

// Starts up a pool with two workers emptying it as fast as they can
// The pool is Stop()ped while jobs are still going on.  Tests that the
// pool processing is really stopped and that it's not first completely
// drained before returning.
// https://github.com/gocraft/work/issues/24
func TestWorkerPoolStop(t *testing.T) {
	ns := "will_it_end"
	pool := newTestPool(":6379")
	var started, stopped int32
	num_iters := 30

	wp := NewWorkerPool(emptyCtx{}, 2, ns, pool)

	wp.Job("sample_job", func (c *emptyCtx, job *Job) error {
		atomic.AddInt32(&started,1)
		time.Sleep(1 * time.Second)
		atomic.AddInt32(&stopped,1)
		return nil
	})

	var enqueuer = work.NewEnqueuer(ns, pool)

	for i := 0; i <= num_iters; i++ {
		enqueuer.Enqueue("sample_job", work.Q{})
	}

	// Start the pool and quit before it has had a chance to complete
	// all the jobs.
	wp.Start()
	time.Sleep(5 * time.Second)
	wp.Stop()

	if started != stopped {
		t.Errorf("Expected that jobs were finished and not killed while processing (started=%d, stopped=%d)",started,stopped)
	}

	if started >= int32(num_iters) {
		t.Errorf("Expected that jobs queue was not completely emptied.")
	}
}

package work

import (
	"github.com/garyburd/redigo/redis"
	"github.com/robfig/cron"
	"sort"
	"sync"
)

// WorkerPool represents a pool of workers. It forms the primary API of gocraft/work. WorkerPools provide the public API of gocraft/work. You can attach jobs and middlware to them. You can start and stop them. Based on their concurrency setting, they'll spin up N worker goroutines.
type WorkerPool struct {
	workerPoolID string
	concurrency  uint
	namespace    string // eg, "myapp-work"
	pool         *redis.Pool

	jobTypes     map[string]*jobType
	middleware   []Middleware
	started      bool
	periodicJobs []*periodicJob

	workers          []*worker
	heartbeater      *workerPoolHeartbeater
	retrier          *requeuer
	scheduler        *requeuer
	deadPoolReaper   *deadPoolReaper
	periodicEnqueuer *periodicEnqueuer
}

type Handler func(ctx *Context) error

type jobType struct {
	Name string
	JobOptions
	Handler Handler
}

// You may provide your own backoff function for retrying failed jobs or use the builtin one.
// Returns the number of seconds to wait until the next attempt.
//
// The builtin backoff calculator provides an exponentially increasing wait function.
type BackoffCalculator func(job *Job) int64

// JobOptions can be passed to JobWithOptions.
type JobOptions struct {
	Priority uint              // Priority from 1 to 10000
	MaxFails uint              // 1: send straight to dead (unless SkipDead)
	SkipDead bool              // If true, don't send failed jobs to the dead queue when retries are exhausted.
	Backoff  BackoffCalculator // If not set, uses the default backoff algorithm
}

// NextMiddlewareFunc is a function type (whose instances are named 'next') that you call to advance to the next middleware.
type NextMiddlewareFunc func() error

// NewWorkerPool creates a new worker pool. ctx should be a struct literal whose type will be used for middleware and handlers. concurrency specifies how many workers to spin up - each worker can process jobs concurrently.
func NewWorkerPool(concurrency uint, namespace string, pool *redis.Pool) *WorkerPool {
	if pool == nil {
		panic("NewWorkerPool needs a non-nil *redis.Pool")
	}

	wp := &WorkerPool{
		workerPoolID: makeIdentifier(),
		concurrency:  concurrency,
		namespace:    namespace,
		pool:         pool,
		jobTypes:     make(map[string]*jobType),
	}

	for i := uint(0); i < wp.concurrency; i++ {
		w := newWorker(wp.namespace, wp.workerPoolID, wp.pool, nil, wp.jobTypes)
		wp.workers = append(wp.workers, w)
	}

	return wp
}

type Middleware func(ctx *Context, next NextMiddlewareFunc) error

// Middleware appends the specified function to the middleware chain. The fn can take one of these forms:
// (*ContextType).func(*Job, NextMiddlewareFunc) error, (ContextType matches the type of ctx specified when creating a pool)
// func(*Job, NextMiddlewareFunc) error, for the generic middleware format.
func (wp *WorkerPool) Middleware(mw Middleware) *WorkerPool {
	wp.middleware = append(wp.middleware, mw)

	for _, w := range wp.workers {
		w.updateMiddlewareAndJobTypes(wp.middleware, wp.jobTypes)
	}

	return wp
}

// Job registers the job name to the specified handler fn. For instnace, when workers pull jobs from the name queue, they'll be processed by the specified handler function.
// fn can take one of these forms:
// (*ContextType).func(*Job) error, (ContextType matches the type of ctx specified when creating a pool)
// func(*Job) error, for the generic handler format.
func (wp *WorkerPool) Job(name string, handler Handler) *WorkerPool {
	return wp.JobWithOptions(name, JobOptions{}, handler)
}

// JobWithOptions adds a handler for 'name' jobs as per the Job function, but permits you specify additional options such as a job's priority, retry count, and whether to send dead jobs to the dead job queue or trash them.
func (wp *WorkerPool) JobWithOptions(name string, jobOpts JobOptions, handler Handler) *WorkerPool {
	jobOpts = applyDefaultsAndValidate(jobOpts)

	jt := &jobType{
		Name:       name,
		Handler:    handler,
		JobOptions: jobOpts,
	}

	wp.jobTypes[name] = jt

	for _, w := range wp.workers {
		w.updateMiddlewareAndJobTypes(wp.middleware, wp.jobTypes)
	}

	return wp
}

// PeriodicallyEnqueue will periodically enqueue jobName according to the cron-based spec.
// The spec format is based on https://godoc.org/github.com/robfig/cron, which is a relatively standard cron format.
// Note that the first value is the seconds!
// If you have multiple worker pools on different machines, they'll all coordinate and only enqueue your job once.
func (wp *WorkerPool) PeriodicallyEnqueue(spec string, jobName string) *WorkerPool {
	schedule, err := cron.Parse(spec)
	if err != nil {
		panic(err)
	}

	wp.periodicJobs = append(wp.periodicJobs, &periodicJob{jobName: jobName, spec: spec, schedule: schedule})

	return wp
}

// Start starts the workers and associated processes.
func (wp *WorkerPool) Start() {
	if wp.started {
		return
	}
	wp.started = true

	go wp.writeKnownJobsToRedis()
	for _, w := range wp.workers {
		go w.start()
	}

	wp.heartbeater = newWorkerPoolHeartbeater(wp.namespace, wp.pool, wp.workerPoolID, wp.jobTypes, wp.concurrency, wp.workerIDs())
	wp.heartbeater.start()
	wp.startRequeuers()
	wp.periodicEnqueuer = newPeriodicEnqueuer(wp.namespace, wp.pool, wp.periodicJobs)
	wp.periodicEnqueuer.start()
}

// Stop stops the workers and associated processes.
func (wp *WorkerPool) Stop() {
	if !wp.started {
		return
	}
	wp.started = false

	wg := sync.WaitGroup{}
	for _, w := range wp.workers {
		wg.Add(1)
		go func(w *worker) {
			w.stop()
			wg.Done()
		}(w)
	}
	wg.Wait()
	wp.heartbeater.stop()
	wp.retrier.stop()
	wp.scheduler.stop()
	wp.deadPoolReaper.stop()
	wp.periodicEnqueuer.stop()
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (wp *WorkerPool) Drain() {
	wg := sync.WaitGroup{}
	for _, w := range wp.workers {
		wg.Add(1)
		go func(w *worker) {
			w.drain()
			wg.Done()
		}(w)
	}
	wg.Wait()
}

func (wp *WorkerPool) startRequeuers() {
	jobNames := make([]string, 0, len(wp.jobTypes))
	for k := range wp.jobTypes {
		jobNames = append(jobNames, k)
	}
	wp.retrier = newRequeuer(wp.namespace, wp.pool, redisKeyRetry(wp.namespace), jobNames)
	wp.scheduler = newRequeuer(wp.namespace, wp.pool, redisKeyScheduled(wp.namespace), jobNames)
	wp.deadPoolReaper = newDeadPoolReaper(wp.namespace, wp.pool)
	wp.retrier.start()
	wp.scheduler.start()
	wp.deadPoolReaper.start()
}

func (wp *WorkerPool) workerIDs() []string {
	wids := make([]string, 0, len(wp.workers))
	for _, w := range wp.workers {
		wids = append(wids, w.workerID)
	}
	sort.Strings(wids)
	return wids
}

func (wp *WorkerPool) writeKnownJobsToRedis() {
	if len(wp.jobTypes) == 0 {
		return
	}

	conn := wp.pool.Get()
	defer conn.Close()

	key := redisKeyKnownJobs(wp.namespace)

	jobNames := make([]interface{}, 0, len(wp.jobTypes)+1)
	jobNames = append(jobNames, key)
	for k := range wp.jobTypes {
		jobNames = append(jobNames, k)
	}

	_, err := conn.Do("SADD", jobNames...)
	if err != nil {
		logError("write_known_jobs", err)
	}
}

func applyDefaultsAndValidate(jobOpts JobOptions) JobOptions {
	if jobOpts.Priority == 0 {
		jobOpts.Priority = 1
	}

	if jobOpts.MaxFails == 0 {
		jobOpts.MaxFails = 4
	}

	if jobOpts.Priority > 100000 {
		panic("work: JobOptions.Priority must be between 1 and 100000")
	}

	return jobOpts
}

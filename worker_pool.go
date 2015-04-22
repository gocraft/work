package work

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	// "fmt"
	"sort"
	"sync"
)

type WorkerPool struct {
	workerPoolID string
	concurrency  uint
	namespace    string // eg, "myapp-work"
	pool         *redis.Pool

	contextType reflect.Type
	jobTypes    map[string]*jobType

	workers   []*worker
	heartbeater *workerPoolHeartbeater
	retrier   *requeuer
	scheduler *requeuer
}

// rename to NewPool?
func NewWorkerPool(ctx interface{}, concurrency uint, namespace string, pool *redis.Pool) *WorkerPool {
	// todo: validate ctx
	// todo: validate concurrency
	workerPoolID := makeIdentifier()
	wp := &WorkerPool{
		workerPoolID: workerPoolID,
		concurrency:  concurrency,
		namespace:    namespace,
		pool:         pool,
		contextType:  reflect.TypeOf(ctx),
		jobTypes:     make(map[string]*jobType),
	}

	for i := uint(0); i < wp.concurrency; i++ {
		w := newWorker(wp.namespace, wp.pool, wp.jobTypes)
		wp.workers = append(wp.workers, w)
	}

	return wp
}

func (wp *WorkerPool) Middleware() *WorkerPool {
	return wp
}

func (wp *WorkerPool) Job(name string, fn interface{}) *WorkerPool {
	return wp.JobWithOptions(name, JobOptions{Priority: 1, MaxFails: 3}, fn)
}

// TODO: depending on how many JobOptions there are it might be good to explode the options
// because it's super awkward for omitted Priority and MaxRetries to be zero-valued
func (wp *WorkerPool) JobWithOptions(name string, jobOpts JobOptions, fn interface{}) *WorkerPool {
	jt := &jobType{
		Name:           name,
		DynamicHandler: reflect.ValueOf(fn),
		JobOptions:     jobOpts,
	}
	if gh, ok := fn.(func(*Job) error); ok {
		jt.IsGeneric = true
		jt.GenericHandler = gh
	}

	wp.jobTypes[name] = jt

	for _, w := range wp.workers {
		w.updateJobTypes(wp.jobTypes)
	}

	return wp
}

func (wp *WorkerPool) Start() {
	go wp.writeKnownJobsToRedis()
	// todo: what if already started?
	for _, w := range wp.workers {
		go w.start()
	}

	wp.heartbeater = newWorkerPoolHeartbeater(wp.namespace, wp.pool, wp.workerPoolID, wp.jobTypes, wp.concurrency, wp.workerIDs())
	wp.heartbeater.start()
	wp.startRequeuers()
}

func (wp *WorkerPool) Stop() {
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
}

func (wp *WorkerPool) Join() {
	for _, w := range wp.workers {
		w.join()
	}
}

func (wp *WorkerPool) startRequeuers() {
	jobNames := make([]string, 0, len(wp.jobTypes))
	for k, _ := range wp.jobTypes {
		jobNames = append(jobNames, k)
	}
	wp.retrier = newRequeuer(wp.namespace, wp.pool, redisKeyRetry(wp.namespace), jobNames)
	wp.scheduler = newRequeuer(wp.namespace, wp.pool, redisKeyScheduled(wp.namespace), jobNames)
	wp.retrier.start()
	wp.scheduler.start()
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
	for k, _ := range wp.jobTypes {
		jobNames = append(jobNames, k)
	}

	_, err := conn.Do("SADD", jobNames...)
	if err != nil {
		logError("write_known_jobs", err)
	}
}

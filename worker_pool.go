package work

import (
	"reflect"

	"github.com/garyburd/redigo/redis"
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
	middleware  []*middlewareHandler

	workers        []*worker
	heartbeater    *workerPoolHeartbeater
	retrier        *requeuer
	scheduler      *requeuer
	deadPoolReaper *deadPoolReaper
}

type jobType struct {
	Name string
	JobOptions

	IsGeneric      bool
	GenericHandler GenericHandler
	DynamicHandler reflect.Value
}

type JobOptions struct {
	Priority uint
	MaxFails uint // 0: send straight to dead (unless SkipDead)
	SkipDead bool //
}

type GenericHandler func(*Job) error
type GenericMiddlewareHandler func(*Job, NextMiddlewareFunc) error
type NextMiddlewareFunc func() error

type middlewareHandler struct {
	IsGeneric                bool
	DynamicMiddleware        reflect.Value
	GenericMiddlewareHandler GenericMiddlewareHandler
}

func NewWorkerPool(ctx interface{}, concurrency uint, namespace string, pool *redis.Pool) *WorkerPool {
	ctxType := reflect.TypeOf(ctx)
	validateContextType(ctxType)
	wp := &WorkerPool{
		workerPoolID: makeIdentifier(),
		concurrency:  concurrency,
		namespace:    namespace,
		pool:         pool,
		contextType:  ctxType,
		jobTypes:     make(map[string]*jobType),
	}

	for i := uint(0); i < wp.concurrency; i++ {
		w := newWorker(wp.namespace, wp.workerPoolID, wp.pool, wp.contextType, nil, wp.jobTypes)
		wp.workers = append(wp.workers, w)
	}

	return wp
}

func (wp *WorkerPool) Middleware(fn interface{}) *WorkerPool {
	vfn := reflect.ValueOf(fn)
	validateMiddlewareType(wp.contextType, vfn)

	mw := &middlewareHandler{
		DynamicMiddleware: vfn,
	}

	if gmh, ok := fn.(func(*Job, NextMiddlewareFunc) error); ok {
		mw.IsGeneric = true
		mw.GenericMiddlewareHandler = gmh
	}

	wp.middleware = append(wp.middleware, mw)

	for _, w := range wp.workers {
		w.updateMiddlewareAndJobTypes(wp.middleware, wp.jobTypes)
	}

	return wp
}

func (wp *WorkerPool) Job(name string, fn interface{}) *WorkerPool {
	return wp.JobWithOptions(name, JobOptions{Priority: 1, MaxFails: 3}, fn)
}

// TODO: depending on how many JobOptions there are it might be good to explode the options
// because it's super awkward for omitted Priority and MaxRetries to be zero-valued
func (wp *WorkerPool) JobWithOptions(name string, jobOpts JobOptions, fn interface{}) *WorkerPool {
	vfn := reflect.ValueOf(fn)
	validateHandlerType(wp.contextType, vfn)
	jt := &jobType{
		Name:           name,
		DynamicHandler: vfn,
		JobOptions:     jobOpts,
	}
	if gh, ok := fn.(func(*Job) error); ok {
		jt.IsGeneric = true
		jt.GenericHandler = gh
	}

	wp.jobTypes[name] = jt

	for _, w := range wp.workers {
		w.updateMiddlewareAndJobTypes(wp.middleware, wp.jobTypes)
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
	wp.deadPoolReaper.stop()
}

func (wp *WorkerPool) Join() {
	wg := sync.WaitGroup{}
	for _, w := range wp.workers {
		wg.Add(1)
		go func(w *worker) {
			w.join()
			wg.Done()
		}(w)
	}
	wg.Wait()
}

func (wp *WorkerPool) startRequeuers() {
	jobNames := make([]string, 0, len(wp.jobTypes))
	for k, _ := range wp.jobTypes {
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
	for k, _ := range wp.jobTypes {
		jobNames = append(jobNames, k)
	}

	_, err := conn.Do("SADD", jobNames...)
	if err != nil {
		logError("write_known_jobs", err)
	}
}

func newJobTypeGeneric(name string, opts JobOptions, handler GenericHandler) *jobType {
	return &jobType{
		Name:           name,
		JobOptions:     opts,
		IsGeneric:      true,
		GenericHandler: handler,
	}
}

// validateContextType will panic if context is invalid
func validateContextType(ctxType reflect.Type) {
	if ctxType.Kind() != reflect.Struct {
		panic("work: Context needs to be a struct type")
	}
}

func validateHandlerType(ctxType reflect.Type, vfn reflect.Value) {
	if !isValidHandlerType(ctxType, vfn) {
		panic("invalid handler passed") // TODO: write a nice message
	}
}

func validateMiddlewareType(ctxType reflect.Type, vfn reflect.Value) {
	if !isValidMiddlewareType(ctxType, vfn) {
		panic("invalid middleware passed") // TODO: write a nice message
	}
}

func isValidHandlerType(ctxType reflect.Type, vfn reflect.Value) bool {
	fnType := vfn.Type()

	if fnType.Kind() != reflect.Func {
		return false
	}

	numIn := fnType.NumIn()
	numOut := fnType.NumOut()

	if numOut != 1 {
		return false
	}

	outType := fnType.Out(0)
	var e *error

	if outType != reflect.TypeOf(e).Elem() {
		return false
	}

	var j *Job
	if numIn == 1 {
		if fnType.In(0) != reflect.TypeOf(j) {
			return false
		}
	} else if numIn == 2 {
		if fnType.In(0) != reflect.PtrTo(ctxType) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(j) {
			return false
		}
	} else {
		return false
	}

	return true
}

func isValidMiddlewareType(ctxType reflect.Type, vfn reflect.Value) bool {
	fnType := vfn.Type()

	if fnType.Kind() != reflect.Func {
		return false
	}

	numIn := fnType.NumIn()
	numOut := fnType.NumOut()

	if numOut != 1 {
		return false
	}

	outType := fnType.Out(0)
	var e *error

	if outType != reflect.TypeOf(e).Elem() {
		return false
	}

	var j *Job
	var nfn NextMiddlewareFunc
	if numIn == 2 {
		if fnType.In(0) != reflect.TypeOf(j) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(nfn) {
			return false
		}
	} else if numIn == 3 {
		if fnType.In(0) != reflect.PtrTo(ctxType) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(j) {
			return false
		}
		if fnType.In(2) != reflect.TypeOf(nfn) {
			return false
		}
	} else {
		return false
	}

	return true
}

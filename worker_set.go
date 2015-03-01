package work

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	// "fmt"
)

type WorkerSet struct {
	concurrency uint
	namespace   string // eg, "myapp-work"
	pool        *redis.Pool

	contextType reflect.Type
	jobTypes    map[string]*jobType

	workers []*worker
}

func NewWorkerSet(ctx interface{}, concurrency uint, namespace string, pool *redis.Pool) *WorkerSet {
	// todo: validate ctx
	// todo: validate concurrency
	ws := &WorkerSet{
		concurrency: concurrency,
		namespace:   namespace,
		pool:        pool,
		contextType: reflect.TypeOf(ctx),
		jobTypes:    make(map[string]*jobType),
	}

	return ws
}

func (ws *WorkerSet) Middleware() *WorkerSet {
	return ws
}

func (ws *WorkerSet) Job(name string, fn interface{}) *WorkerSet {
	return ws.JobWithOptions(name, JobOptions{Priority: 1, MaxFails: 3}, fn)
}

// TODO: depending on how many JobOptions there are it might be good to explode the options
// because it's super awkward for Priority and MaxRetries to be zero-valued
func (ws *WorkerSet) JobWithOptions(name string, jobOpts JobOptions, fn interface{}) *WorkerSet {
	jt := &jobType{
		Name:           name,
		DynamicHandler: reflect.ValueOf(fn),
		JobOptions:     jobOpts,
	}
	if gh, ok := fn.(func(*Job) error); ok {
		jt.IsGeneric = true
		jt.GenericHandler = gh
	}

	ws.jobTypes[name] = jt
	return ws
}

func (ws *WorkerSet) Start() {
	// todo: what if already started?
	for i := uint(0); i < ws.concurrency; i++ {
		w := newWorker(ws.namespace, ws.pool, ws.jobTypes)
		ws.workers = append(ws.workers, w)
		w.start()
	}
}

func (ws *WorkerSet) Stop() {
}

func (ws *WorkerSet) Join() {
	for _, w := range ws.workers {
		w.join()
	}
}

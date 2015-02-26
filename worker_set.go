package work

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
)

type WorkerSet struct {
	concurrency uint
	namespace   string // eg, "myapp-work"
	pool        *redis.Pool

	contextType reflect.Type
	jobTypes    []*jobType
}

func NewWorkerSet(ctx interface{}, concurrency uint, namespace string, pool *redis.Pool) *WorkerSet {
	// todo: validate ctx
	// todo: validate concurrency
	ws := &WorkerSet{
		concurrency: concurrency,
		namespace:   namespace,
		pool:        pool,
		contextType: reflect.TypeOf(ctx),
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
	ws.jobTypes = append(ws.jobTypes, jt)
	return ws
}

func (ws *WorkerSet) Start() {
	// for _, jt := range ws.jobTypes {
	// 	go
	// }
}

func (ws *WorkerSet) Stop() {
}

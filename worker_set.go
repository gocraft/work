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

type jobType struct {
	Name    string
	Handler reflect.Value
	JobOptions
}

type JobOptions struct {
	Priority uint
	// retry count
	//
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
	return ws.JobWithOptions(name, JobOptions{Priority: 1}, fn)
}

func (ws *WorkerSet) JobWithOptions(name string, jobOpts JobOptions, fn interface{}) *WorkerSet {
	jt := &jobType{
		Name:        name,
		Handler:     reflect.ValueOf(fn),
		JobOptions:  jobOpts,
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

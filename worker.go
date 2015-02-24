package work

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
)

type worker struct {
	ws *WorkerSet

	namespace string // eg, "myapp-work"
	pool      *redis.Pool
	jobTypes  []*jobType

	redisFetchScript *redis.Script
	sampler prioritySampler

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newWorker(ws *WorkerSet) *worker {
	sampler := prioritySampler{}
	for _, jt := range ws.jobTypes {
		sampler.add(jt.Priority, redisKeyJobs(ws.namespace, jt.Name), redisKeyJobsInProgress(ws.namespace, jt.Name))
	}

	return &worker{
		ws: ws,

		namespace: ws.namespace,
		pool:      ws.pool,
		jobTypes:  ws.jobTypes,
		
		redisFetchScript: redis.NewScript(len(ws.jobTypes), redisLuaRpoplpushMultiCmd),
		sampler: sampler,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (w *worker) start() {
	go w.loop()
}

func (w *worker) stop() {
	close(w.stopChan)
	<-w.doneStoppingChan
}

func (w *worker) loop() {
	for {
		select {
		case <-w.stopChan:
			close(w.doneStoppingChan)
			return
		default:
			job, err := w.fetchJob()
			if err != nil {
				// TODO: log this i think?
			} else if job != nil {
				// DO IT
			} else {
				// no job.
			}
		}
	}
}

func (w *worker) fetchJob() (*Job, error) {
	
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	w.sampler.sample()
	
	var scriptArgs = make([]interface{}, 0, len(w.sampler.samples) * 2)
	for _, s := range w.sampler.samples {
		scriptArgs = append(scriptArgs, s.redisJobs, s.redisJobsInProg)
	}
	
	conn := w.pool.Get()
	defer conn.Close()
	values, err := redis.Values(w.redisFetchScript.Do(conn, scriptArgs...))
	if err == redis.ErrNil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	
	jsonData, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("response msg not bytes")
	}
	
	queue, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("response queue not bytes")
	}
	
	job, err := newJob(jsonData, queue)
	if err != nil {
		return nil, err
	}

	return job, nil
}


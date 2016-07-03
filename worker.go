package work

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
)

type worker struct {
	workerID    string
	poolID      string
	namespace   string
	pool        *redis.Pool
	jobTypes    map[string]*jobType
	middleware  []*middlewareHandler
	contextType reflect.Type

	redisFetchScript *redis.Script
	sampler          prioritySampler
	*observer

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	joinChan        chan struct{}
	doneJoiningChan chan struct{}
}

func newWorker(namespace string, poolID string, pool *redis.Pool, contextType reflect.Type, middleware []*middlewareHandler, jobTypes map[string]*jobType) *worker {
	workerID := makeIdentifier()
	ob := newObserver(namespace, pool, workerID)

	w := &worker{
		workerID:    workerID,
		poolID:      poolID,
		namespace:   namespace,
		pool:        pool,
		contextType: contextType,

		observer: ob,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		joinChan:        make(chan struct{}),
		doneJoiningChan: make(chan struct{}),
	}

	w.updateMiddlewareAndJobTypes(middleware, jobTypes)

	return w
}

// note: can't be called while the thing is started
func (w *worker) updateMiddlewareAndJobTypes(middleware []*middlewareHandler, jobTypes map[string]*jobType) {
	w.middleware = middleware
	sampler := prioritySampler{}
	for _, jt := range jobTypes {
		sampler.add(jt.Priority, redisKeyJobs(w.namespace, jt.Name), redisKeyJobsInProgress(w.namespace, w.poolID, jt.Name))
	}
	w.sampler = sampler
	w.jobTypes = jobTypes
	w.redisFetchScript = redis.NewScript(len(jobTypes)*2, redisLuaRpoplpushMultiCmd)
}

func (w *worker) start() {
	go w.loop()
	go w.observer.start()
}

func (w *worker) stop() {
	close(w.stopChan)
	<-w.doneStoppingChan
	w.observer.join()
	w.observer.stop()
}

func (w *worker) join() {
	w.joinChan <- struct{}{}
	<-w.doneJoiningChan
	w.observer.join()
}

var sleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 5000}

func (w *worker) loop() {
	var joined bool
	var consequtiveNoJobs int64
	var nextTry time.Time
	const sleepIncrement = 10 * time.Millisecond
	for {
		select {
		case <-w.stopChan:
			close(w.doneStoppingChan)
			return
		case <-w.joinChan:
			joined = true
		default:
			if nextTry.IsZero() {
				job, err := w.fetchJob()
				if err != nil {
					logError("worker.fetch", err)
				} else if job != nil {
					w.processJob(job)
					consequtiveNoJobs = 0
				} else {
					if joined {
						w.doneJoiningChan <- struct{}{}
						joined = false
					}
					consequtiveNoJobs++
					idx := consequtiveNoJobs
					if idx >= int64(len(sleepBackoffsInMilliseconds)) {
						idx = int64(len(sleepBackoffsInMilliseconds)) - 1
					}
					nextTry = time.Now().Add(time.Duration(sleepBackoffsInMilliseconds[idx]) * time.Millisecond)
					time.Sleep(sleepIncrement)
				}
			} else {
				if time.Now().After(nextTry) {
					nextTry = time.Time{}
				} else {
					time.Sleep(sleepIncrement)
				}
			}
		}
	}
}

func (w *worker) fetchJob() (*Job, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	w.sampler.sample()

	var scriptArgs = make([]interface{}, 0, len(w.sampler.samples)*2)
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

	if len(values) != 3 {
		return nil, fmt.Errorf("need 3 elements back")
	}

	rawJSON, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("response msg not bytes")
	}

	dequeuedFrom, ok := values[1].([]byte)
	if !ok {
		return nil, fmt.Errorf("response queue not bytes")
	}

	inProgQueue, ok := values[2].([]byte)
	if !ok {
		return nil, fmt.Errorf("response in prog not bytes")
	}

	job, err := newJob(rawJSON, dequeuedFrom, inProgQueue)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (w *worker) processJob(job *Job) {
	defer w.removeJobFromInProgress(job)
	//fmt.Println("JOB: ", *job, string(job.dequeuedFrom))
	if jt, ok := w.jobTypes[job.Name]; ok {
		w.observeStarted(job.Name, job.ID, job.Args)
		_, runErr := runJob(job, w.contextType, w.middleware, jt)
		w.observeDone(job.Name, job.ID, runErr)
		if runErr != nil {
			job.failed(runErr)
			w.addToRetryOrDead(jt, job, runErr) // TODO: if this fails we shouldn't remove from in-progress
		}
	} else {
		// NOTE: since we don't have a jobType, we don't know max retries
		runErr := fmt.Errorf("stray job -- no handler")
		job.failed(runErr)
		w.addToDead(job, runErr) // TODO: if this fails we shouldn't remove from in-progress
	}
}

func (w *worker) removeJobFromInProgress(job *Job) {
	conn := w.pool.Get()
	defer conn.Close()

	_, err := conn.Do("LREM", job.inProgQueue, 1, job.rawJSON)
	if err != nil {
		logError("worker.remove_job_from_in_progress.lrem", err)
	}
}

func (w *worker) addToRetryOrDead(jt *jobType, job *Job, runErr error) {
	failsRemaining := int64(jt.MaxFails) - job.Fails
	if failsRemaining > 0 {
		w.addToRetry(job, runErr)
	} else {
		if !jt.SkipDead {
			w.addToDead(job, runErr)
		}
	}
}

func (w *worker) addToRetry(job *Job, runErr error) {
	rawJSON, err := job.Serialize()
	if err != nil {
		logError("worker.add_to_retry", err)
		return
	}

	conn := w.pool.Get()
	defer conn.Close()

	_, err = conn.Do("ZADD", redisKeyRetry(w.namespace), nowEpochSeconds()+backoff(job.Fails), rawJSON)
	if err != nil {
		logError("worker.add_to_retry.zadd", err)
	}

}

func (w *worker) addToDead(job *Job, runErr error) {
	rawJSON, err := job.Serialize()

	if err != nil {
		logError("worker.add_to_dead.serialize", err)
		return
	}

	conn := w.pool.Get()
	defer conn.Close()

	_, err = conn.Do("ZADD", redisKeyDead(w.namespace), nowEpochSeconds(), rawJSON)
	// NOTE: sidekiq limits the # of jobs: only keep jobs for 6 months, and only keep a max # of jobs
	// The max # of jobs seems really horrible. Seems like operations should be on top of it.
	// conn.Send("ZREMRANGEBYSCORE", redisKeyDead(w.namespace), "-inf", now - keepInterval)
	// conn.Send("ZREMRANGEBYRANK", redisKeyDead(w.namespace), 0, -maxJobs)
	if err != nil {
		logError("worker.add_to_dead.zadd", err)
	}
}

// backoff returns number of seconds t
func backoff(fails int64) int64 {
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}

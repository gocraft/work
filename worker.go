package work

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"time"
)

type worker struct {
	workerID  string
	namespace string // eg, "myapp-work"
	pool      *redis.Pool
	jobTypes  map[string]*jobType

	redisFetchScript *redis.Script
	sampler          prioritySampler
	*observer

	// nosleep is used for testing. In production use, we want the worker to sleep if it doesn't have any work.
	// In testing, we don't want to waste time sleeping. Ideally, there'd be a more elegant solution, possibly
	// renaming join() to joinAndClose(). Note that in the observer, we don't want joinAndClose, just join.
	nosleep bool

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	joinChan        chan struct{}
	doneJoiningChan chan struct{}
}

func newWorker(namespace string, pool *redis.Pool, jobTypes map[string]*jobType) *worker {
	sampler := prioritySampler{}
	for _, jt := range jobTypes {
		sampler.add(jt.Priority, redisKeyJobs(namespace, jt.Name), redisKeyJobsInProgress(namespace, jt.Name))
	}

	workerID := makeIdentifier()
	ob := newObserver(namespace, workerID, pool)

	return &worker{
		workerID:  workerID,
		namespace: namespace,
		pool:      pool,
		jobTypes:  jobTypes,

		redisFetchScript: redis.NewScript(len(jobTypes)*2, redisLuaRpoplpushMultiCmd),
		sampler:          sampler,
		observer:         ob,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		joinChan:        make(chan struct{}),
		doneJoiningChan: make(chan struct{}),
	}
}

func (w *worker) start() {
	go w.loop()
	go w.observer.start()
}

func (w *worker) stop() {
	close(w.stopChan)
	<-w.doneStoppingChan

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
	for {
		select {
		case <-w.stopChan:
			close(w.doneStoppingChan)
			return
		case <-w.joinChan:
			joined = true
		default:
			job, err := w.fetchJob()
			if err != nil {
				logError("fetch", err)
			} else if job != nil {
				w.processJob(job)
				consequtiveNoJobs = 0
			} else {
				if joined {
					w.doneJoiningChan <- struct{}{}
					joined = false
				} else if !w.nosleep {
					// This is the normal branch in production when we don't find work.
					consequtiveNoJobs++

					sleepMS := sleepBackoffsInMilliseconds[len(sleepBackoffsInMilliseconds)-1]
					if consequtiveNoJobs < int64(len(sleepBackoffsInMilliseconds)) {
						sleepMS = sleepBackoffsInMilliseconds[consequtiveNoJobs]
					}
					sleepMS += rand.Int63n(sleepMS / 2) // jitter
					time.Sleep(time.Duration(sleepMS) * time.Millisecond)
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
		runErr := runJob(job, jt)
		w.observeDone(job.Name, job.ID, runErr)
		if runErr != nil {
			job.failed(runErr)
			w.addToRetryOrDead(jt, job, runErr)
		}
	} else {
		// NOTE: since we don't have a jobType, we don't know max retries
		runErr := fmt.Errorf("stray job -- no handler")
		job.failed(runErr)
		w.addToDead(job, runErr)
	}
}

func (w *worker) removeJobFromInProgress(job *Job) {
	conn := w.pool.Get()
	defer conn.Close()

	_, err := conn.Do("LREM", job.inProgQueue, 1, job.rawJSON)
	if err != nil {
		logError("remove_job_from_in_progress", err)
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
		// todo: log
		return
	}

	conn := w.pool.Get()
	defer conn.Close()

	_, err = conn.Do("ZADD", redisKeyRetry(w.namespace), nowEpochSeconds()+backoff(job.Fails), rawJSON)
	if err != nil {
		logError("add_to_retry.zadd", err)
	}

}

func (w *worker) addToDead(job *Job, runErr error) {
	rawJSON, err := job.Serialize()

	if err != nil {
		// todo: log
		return
	}

	conn := w.pool.Get()
	defer conn.Close()

	_, err = conn.Do("ZADD", redisKeyDead(w.namespace), nowEpochSeconds()+backoff(job.Fails), rawJSON)
	// NOTE: sidekiq limits the # of jobs: only keep jobs for 6 months, and only keep a max # of jobs
	// The max # of jobs seems really horrible. Seems like
	// conn.Send("ZREMRANGEBYSCORE", redisKeyDead(w.namespace), "-inf", now - keepInterval)
	// conn.Send("ZREMRANGEBYRANK", redisKeyDead(w.namespace), 0, -maxJobs)
	if err != nil {
		logError("add_to_dead.zadd", err)
	}
}

// backoff returns number of seconds t
func backoff(fails int64) int64 {
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}

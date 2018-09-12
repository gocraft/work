package work

import (
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Enqueuer can enqueue jobs.
type Enqueuer struct {
	Namespace string // eg, "myapp-work"
	Pool      *redis.Pool

	queuePrefix           string // eg, "myapp-work:jobs:"
	knownJobs             map[string]int64
	enqueueUniqueScript   *redis.Script
	enqueueUniqueInScript *redis.Script
	mtx                   sync.RWMutex
}

// NewEnqueuer creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewEnqueuer(namespace string, pool *redis.Pool) *Enqueuer {
	if pool == nil {
		panic("NewEnqueuer needs a non-nil *redis.Pool")
	}

	return &Enqueuer{
		Namespace:             namespace,
		Pool:                  pool,
		queuePrefix:           redisKeyJobsPrefix(namespace),
		knownJobs:             make(map[string]int64),
		enqueueUniqueScript:   redis.NewScript(2, redisLuaEnqueueUnique),
		enqueueUniqueInScript: redis.NewScript(2, redisLuaEnqueueUniqueIn),
	}
}

// Enqueue will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com"})
func (e *Enqueuer) Enqueue(jobName string, args map[string]interface{}) (*Job, error) {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("LPUSH", e.queuePrefix+jobName, rawJSON); err != nil {
		return nil, err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return job, err
	}

	return job, nil
}

// EnqueueIn enqueues a job in the scheduled job queue for execution in secondsFromNow seconds.
func (e *Enqueuer) EnqueueIn(jobName string, secondsFromNow int64, args map[string]interface{}) (*ScheduledJob, error) {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	scheduledJob := &ScheduledJob{
		RunAt: nowEpochSeconds() + secondsFromNow,
		Job:   job,
	}

	_, err = conn.Do("ZADD", redisKeyScheduled(e.Namespace), scheduledJob.RunAt, rawJSON)
	if err != nil {
		return nil, err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return scheduledJob, err
	}

	return scheduledJob, nil
}

// EnqueueUnique enqueues a job unless a job is already enqueued with the same name and arguments.
// The already-enqueued job can be in the normal work queue or in the scheduled job queue.
// Once a worker begins processing a job, another job with the same name and arguments can be enqueued again.
// Any failed jobs in the retry queue or dead queue don't count against the uniqueness -- so if a job fails and is retried, two unique jobs with the same name and arguments can be enqueued at once.
// In order to add robustness to the system, jobs are only unique for 24 hours after they're enqueued. This is mostly relevant for scheduled jobs.
// EnqueueUnique returns the job if it was enqueued and nil if it wasn't
func (e *Enqueuer) EnqueueUnique(jobName string, args map[string]interface{}) (*Job, error) {
	return e.EnqueueUniqueByKey(jobName, args, nil)
}

// EnqueueUniqueIn enqueues a unique job in the scheduled job queue for execution in secondsFromNow seconds. See EnqueueUnique for the semantics of unique jobs.
func (e *Enqueuer) EnqueueUniqueIn(jobName string, secondsFromNow int64, args map[string]interface{}) (*ScheduledJob, error) {
	return e.EnqueueUniqueInByKey(jobName, secondsFromNow, args, nil)
}

// EnqueueUniqueByKey enqueues a job unless a job is already enqueued with the same name and key, updating arguments.
// The already-enqueued job can be in the normal work queue or in the scheduled job queue.
// Once a worker begins processing a job, another job with the same name and key can be enqueued again.
// Any failed jobs in the retry queue or dead queue don't count against the uniqueness -- so if a job fails and is retried, two unique jobs with the same name and arguments can be enqueued at once.
// In order to add robustness to the system, jobs are only unique for 24 hours after they're enqueued. This is mostly relevant for scheduled jobs.
// EnqueueUniqueByKey returns the job if it was enqueued and nil if it wasn't
func (e *Enqueuer) EnqueueUniqueByKey(jobName string, args map[string]interface{}, keyMap map[string]interface{}) (*Job, error) {
	enqueue, job, err := e.uniqueJobHelper(jobName, args, keyMap)
	if err != nil {
		return nil, err
	}

	res, err := enqueue(nil)

	if res == "ok" && err == nil {
		return job, nil
	}
	return nil, err
}

// EnqueueUniqueInByKey enqueues a job in the scheduled job queue that is unique on specified key for execution in secondsFromNow seconds. See EnqueueUnique for the semantics of unique jobs.
// Subsequent calls with same key will update arguments
func (e *Enqueuer) EnqueueUniqueInByKey(jobName string, secondsFromNow int64, args map[string]interface{}, keyMap map[string]interface{}) (*ScheduledJob, error) {
	enqueue, job, err := e.uniqueJobHelper(jobName, args, keyMap)
	if err != nil {
		return nil, err
	}

	scheduledJob := &ScheduledJob{
		RunAt: nowEpochSeconds() + secondsFromNow,
		Job:   job,
	}

	res, err := enqueue(&scheduledJob.RunAt)
	if res == "ok" && err == nil {
		return scheduledJob, nil
	}
	return nil, err
}

func (e *Enqueuer) addToKnownJobs(conn redis.Conn, jobName string) error {
	needSadd := true
	now := time.Now().Unix()

	e.mtx.RLock()
	t, ok := e.knownJobs[jobName]
	e.mtx.RUnlock()

	if ok {
		if now < t {
			needSadd = false
		}
	}
	if needSadd {
		if _, err := conn.Do("SADD", redisKeyKnownJobs(e.Namespace), jobName); err != nil {
			return err
		}

		e.mtx.Lock()
		e.knownJobs[jobName] = now + 300
		e.mtx.Unlock()
	}

	return nil
}

type enqueueFnType func(*int64) (string, error)

func (e *Enqueuer) uniqueJobHelper(jobName string, args map[string]interface{}, keyMap map[string]interface{}) (enqueueFnType, *Job, error) {
	useDefaultKeys := false
	if keyMap == nil {
		useDefaultKeys = true
		keyMap = args
	}

	uniqueKey, err := redisKeyUniqueJob(e.Namespace, jobName, keyMap)
	if err != nil {
		return nil, nil, err
	}

	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
		Unique:     true,
		UniqueKey:  uniqueKey,
	}

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, nil, err
	}

	enqueueFn := func(runAt *int64) (string, error) {
		conn := e.Pool.Get()
		defer conn.Close()

		if err := e.addToKnownJobs(conn, jobName); err != nil {
			return "", err
		}

		scriptArgs := []interface{}{}
		script := e.enqueueUniqueScript

		scriptArgs = append(scriptArgs, e.queuePrefix+jobName) // KEY[1]
		scriptArgs = append(scriptArgs, uniqueKey)             // KEY[2]
		scriptArgs = append(scriptArgs, rawJSON)               // ARGV[1]
		if useDefaultKeys {
			// keying on arguments so arguments can't be updated
			// we'll just get them off the original job so to save space, make this "1"
			scriptArgs = append(scriptArgs, "1") // ARGV[2]
		} else {
			// we'll use this for updated arguments since the job on the queue
			// doesn't get updated
			scriptArgs = append(scriptArgs, rawJSON) // ARGV[2]
		}

		if runAt != nil { // Scheduled job so different job queue with additional arg
			scriptArgs[0] = redisKeyScheduled(e.Namespace) // KEY[1]
			scriptArgs = append(scriptArgs, *runAt)        // ARGV[3]

			script = e.enqueueUniqueInScript
		}

		return redis.String(script.Do(conn, scriptArgs...))
	}

	return enqueueFn, job, nil
}

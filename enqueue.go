package work

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

// Enqueuer can enqueue jobs.
type Enqueuer struct {
	Namespace string // eg, "myapp-work"
	Pool      *redis.Pool

	queuePrefix           string // eg, "myapp-work:jobs:"
	knownJobs             map[string]int64
	enqueueUniqueScript   *redis.Script
	enqueueUniqueInScript *redis.Script
}

// NewEnqueuer creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewEnqueuer(namespace string, pool *redis.Pool) *Enqueuer {
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
func (e *Enqueuer) Enqueue(jobName string, args map[string]interface{}) error {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}

	rawJSON, err := job.serialize()
	if err != nil {
		return err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("LPUSH", e.queuePrefix+jobName, rawJSON); err != nil {
		return err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return err
	}

	return nil
}

// EnqueueIn enqueues a job in the scheduled job queue for execution in secondsFromNow seconds.
func (e *Enqueuer) EnqueueIn(jobName string, secondsFromNow int64, args map[string]interface{}) error {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}

	rawJSON, err := job.serialize()
	if err != nil {
		return err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("ZADD", redisKeyScheduled(e.Namespace), nowEpochSeconds()+secondsFromNow, rawJSON)
	if err != nil {
		return err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return err
	}

	return nil
}

// EnqueueUnique enqueues a job unless a job is already enqueued with the same name and arguments. The already-enqueued job can be in the normal work queue or in the scheduled job queue. Once a worker begins processing a job, another job with the same name and arguments can be enqueued again. Any failed jobs in the retry queue or dead queue don't count against the uniqueness -- so if a job fails and is retried, two unique jobs with the same name and arguments can be enqueued at once.
// In order to add robustness to the system, jobs are only unique for 24 hours after they're enqueued. This is mostly relevant for schedule jobs.
// EnqueueUnique returns true if the job is enqueued and false if it wasn't enqueued.
func (e *Enqueuer) EnqueueUnique(jobName string, args map[string]interface{}) (bool, error) {
	uniqueKey, err := redisKeyUniqueJob(e.Namespace, jobName, args)
	if err != nil {
		return false, err
	}

	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
		Unique:     true,
	}

	rawJSON, err := job.serialize()
	if err != nil {
		return false, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return false, err
	}

	scriptArgs := make([]interface{}, 0, 3)
	scriptArgs = append(scriptArgs, e.queuePrefix+jobName) // KEY[1]
	scriptArgs = append(scriptArgs, uniqueKey)             // KEY[2]
	scriptArgs = append(scriptArgs, rawJSON)

	res, err := redis.String(e.enqueueUniqueScript.Do(conn, scriptArgs...))

	return res == "ok", err
}

// EnqueueUniqueIn enqueues a unique job in the scheduled job queue for execution in secondsFromNow seconds. See EnqueueUnique for the semantics of unique jobs.
func (e *Enqueuer) EnqueueUniqueIn(jobName string, secondsFromNow int64, args map[string]interface{}) (bool, error) {
	uniqueKey, err := redisKeyUniqueJob(e.Namespace, jobName, args)
	if err != nil {
		return false, err
	}

	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
		Unique:     true,
	}

	rawJSON, err := job.serialize()
	if err != nil {
		return false, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return false, err
	}

	scriptArgs := make([]interface{}, 0, 4)
	scriptArgs = append(scriptArgs, redisKeyScheduled(e.Namespace))   // KEY[1]
	scriptArgs = append(scriptArgs, uniqueKey)                        // KEY[2]
	scriptArgs = append(scriptArgs, rawJSON)                          // ARGV[1]
	scriptArgs = append(scriptArgs, nowEpochSeconds()+secondsFromNow) // ARGV[2]

	res, err := redis.String(e.enqueueUniqueInScript.Do(conn, scriptArgs...))

	return res == "ok", err
}

func (e *Enqueuer) addToKnownJobs(conn redis.Conn, jobName string) error {
	needSadd := true
	now := time.Now().Unix()
	t, ok := e.knownJobs[jobName]
	if ok {
		if now < t {
			needSadd = false
		}
	}
	if needSadd {
		if _, err := conn.Do("SADD", redisKeyKnownJobs(e.Namespace), jobName); err != nil {
			return err
		}
		e.knownJobs[jobName] = now + 300
	}

	return nil
}

package work

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

// Enqueuer can enqueue jobs.
type Enqueuer struct {
	Namespace string // eg, "myapp-work"
	Pool      *redis.Pool

	queuePrefix string // eg, "myapp-work:jobs:"
}

// NewEnqueuer creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewEnqueuer(namespace string, pool *redis.Pool) *Enqueuer {
	return &Enqueuer{Namespace: namespace, Pool: pool, queuePrefix: redisKeyJobsPrefix(namespace)}
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

	_, err = conn.Do("LPUSH", e.queuePrefix+jobName, rawJSON)
	if err != nil {
		fmt.Println("got err in enqueue: ", err)
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
		fmt.Println("got err in enqueue: ", err)
		return err
	}

	return nil
}

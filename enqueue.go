package work

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
)

type Enqueuer struct {
	Namespace string // eg, "myapp-work"
	Pool      *redis.Pool

	queuePrefix string // eg, "myapp-work:jobs:"
}

func NewEnqueuer(namespace string, pool *redis.Pool) *Enqueuer {
	return &Enqueuer{Namespace: namespace, Pool: pool, queuePrefix: redisKeyJobsPrefix(namespace)}
}

func (e *Enqueuer) Enqueue(jobName string, args ...interface{}) error {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}

	rawJSON, err := job.Serialize()
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

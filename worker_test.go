package work

import (
	// "fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"testing"
	// "time"
)

func TestWorker(t *testing.T) {
	pool := newTestPool(":6379")

	ns := "work"
	job1 := "job1"
	job2 := "job2"
	job3 := "job3"

	deleteQueue(pool, ns, job1)
	deleteQueue(pool, ns, job2)
	deleteQueue(pool, ns, job3)
	deleteRetryAndDead(pool, ns)

	var arg1 float64
	var arg2 float64
	var arg3 float64

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg1 = job.Args[0].(float64)
			return nil
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg2 = job.Args[0].(float64)
			return nil
		},
	}
	jobTypes[job3] = &jobType{
		Name:       job3,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg3 = job.Args[0].(float64)
			return nil
		},
	}

	enqueuer := NewEnqueuer("work", pool)
	err := enqueuer.Enqueue(job1, 1)
	assert.Nil(t, err)
	err = enqueuer.Enqueue(job2, 2)
	assert.Nil(t, err)
	err = enqueuer.Enqueue(job3, 3)
	assert.Nil(t, err)

	w := newWorker(ns, pool, jobTypes)
	w.start()
	w.forceIter() // make sure it processes the job
	w.forceIter() // make sure it processes the job
	w.forceIter() // make sure it processes the job
	w.stop()

	assert.Equal(t, 1.0, arg1)
	assert.Equal(t, 2.0, arg2)
	assert.Equal(t, 3.0, arg3)

	// assert: nothing in retries or dead
	assert.Equal(t, 0, zsetSize(pool, redisKeyRetry(ns)))
	assert.Equal(t, 0, zsetSize(pool, redisKeyDead(ns)))

	// Nothing in the queues or in-progress queues
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job2)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, job3)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job1)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job2)))
	assert.Equal(t, 0, listSize(pool, redisKeyJobsInProgress(ns, job3)))
}

func deleteQueue(pool *redis.Pool, namespace, jobName string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

func deleteRetryAndDead(pool *redis.Pool, namespace string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyRetry(namespace), redisKeyDead(namespace))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
}

func zsetSize(pool *redis.Pool, key string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("ZCARD", key))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
	return v
}

func listSize(pool *redis.Pool, key string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("LLEN", key))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
	return v
}

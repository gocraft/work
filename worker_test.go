package work

import (
	// "fmt"
	"github.com/stretchr/testify/assert"
	"github.com/garyburd/redigo/redis"
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
	
	var arg1 float64
	var arg2 float64
	var arg3 float64

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric: true,
		GenericHandler: func(job *Job) error {
			arg1 = job.Args[0].(float64)
			return nil
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric: true,
		GenericHandler: func(job *Job) error {
			arg2 = job.Args[0].(float64)
			return nil
		},
		
	}
	jobTypes[job3] = &jobType{
		Name:       job3,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric: true,
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
	w.forceIter() // make sure it process the job
	w.forceIter() // make sure it process the job
	w.forceIter() // make sure it process the job
	w.stop()
	
	assert.Equal(t, arg1, 1.0)
	assert.Equal(t, arg2, 2.0)
	assert.Equal(t, arg3, 3.0)
}

func deleteQueue(pool *redis.Pool, namespace, jobName string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

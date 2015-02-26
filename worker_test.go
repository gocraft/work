package work

import (
	// "fmt"
	// "github.com/stretchr/testify/assert"
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
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

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1},
	}
	jobTypes[job3] = &jobType{
		Name:       job3,
		JobOptions: JobOptions{Priority: 1},
	}

	w := newWorker(ns, pool, jobTypes)
	w.start()

	enqueuer := NewEnqueuer("work", pool)
	_ = enqueuer.Enqueue(job1, 1, "cool")

	time.Sleep(10 * time.Millisecond)

}

func deleteQueue(pool *redis.Pool, namespace, jobName string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

package work

import (
	"github.com/gomodule/redigo/redis"
)

func DeleteQueue(pool *redis.Pool, namespace, jobName string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, "1", jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

func PauseJobs(namespace, jobName string, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("SET", redisKeyJobsPaused(namespace, jobName), "1"); err != nil {
		return err
	}
	return nil
}

func UnpauseJobs(namespace, jobName string, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("DEL", redisKeyJobsPaused(namespace, jobName)); err != nil {
		return err
	}
	return nil
}
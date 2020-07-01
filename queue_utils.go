package work

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strings"
)

func DeleteQueue(pool *redis.Pool, namespace, jobName string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, "1", jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

func DeleteJob(namespace, jobName string, jobId string, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()
	items, err := redis.Values(conn.Do("LRANGE", redisKeyJobs(namespace, jobName), 0, -1))
	if err != nil {
		panic("ERROR deleting job! : " + err.Error())
	}

	for _, item := range items {
		v, _ := redis.String(item, err)
		if strings.Contains(v, jobId) {
			out, _ := conn.Do("LREM", redisKeyJobs(namespace, jobName), 1, v)
			fmt.Println(out)
			return
		}
	}
	panic("couldn't find jobID")
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
package work

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	deadTime          = 10 * time.Second // 2 x heartbeat
	reapPeriod        = 10 * time.Minute
	reapJitterSecs    = 30
	requeueKeysPerJob = 4
)

type deadPoolReaper struct {
	namespace   string
	pool        Pool
	deadTime    time.Duration
	reapPeriod  time.Duration
	curJobTypes []string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newDeadPoolReaper(namespace string, pool Pool, curJobTypes []string) *deadPoolReaper {
	return &deadPoolReaper{
		namespace:        namespace,
		pool:             pool,
		deadTime:         deadTime,
		reapPeriod:       reapPeriod,
		curJobTypes:      curJobTypes,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (r *deadPoolReaper) start() {
	go r.loop()
}

func (r *deadPoolReaper) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *deadPoolReaper) loop() {
	// Reap immediately after we provide some time for initialization
	timer := time.NewTimer(r.deadTime)
	defer timer.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			// Schedule next occurrence periodically with jitter
			timer.Reset(r.reapPeriod + time.Duration(rand.Intn(reapJitterSecs))*time.Second)

			// Reap
			if err := r.reap(); err != nil {
				logError("dead_pool_reaper.reap", err)
			}
		}
	}
}

func (r *deadPoolReaper) reap() (err error) {
	lockValue, err := genValue()
	if err != nil {
		return err
	}

	acquired, err := r.acquireLock(lockValue)
	if err != nil {
		return err
	}

	// Another reaper is already running
	if !acquired {
		return nil
	}

	defer func() {
		err = r.releaseLock(lockValue)
	}()

	deadPoolIDs, err := r.findDeadPools()
	if err != nil {
		return err
	}

	conn := r.pool.Get()
	defer conn.Close()

	// Cleanup all dead pools
	for deadPoolID, jobTypes := range deadPoolIDs {
		lockJobTypes := jobTypes
		// if we found jobs from the heartbeat, requeue them and remove the heartbeat
		if len(jobTypes) > 0 {
			if err = r.requeueInProgressJobs(deadPoolID, jobTypes); err != nil {
				return err
			}

			if _, err = conn.Do("DEL", redisKeyHeartbeat(r.namespace, deadPoolID)); err != nil {
				return err
			}
		} else {
			// try to clean up locks for the current set of jobs if heartbeat was not found
			lockJobTypes = r.curJobTypes
		}

		// Cleanup any stale lock info
		if err = r.cleanStaleLockInfo(deadPoolID, lockJobTypes); err != nil {
			return err
		}

		// Remove dead pool from worker pools set
		if _, err = conn.Do("SREM", redisKeyWorkerPools(r.namespace), deadPoolID); err != nil {
			return err
		}
	}

	return nil
}

func (r *deadPoolReaper) cleanStaleLockInfo(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * 2
	redisReapLocksScript := redis.NewScript(numKeys, redisLuaReapStaleLocks)
	var scriptArgs = make([]interface{}, 0, numKeys+1) // +1 for argv[1]

	for _, jobType := range jobTypes {
		scriptArgs = append(scriptArgs, redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType))
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()
	if _, err := redisReapLocksScript.Do(conn, scriptArgs...); err != nil {
		return err
	}

	return nil
}

func (r *deadPoolReaper) requeueInProgressJobs(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * requeueKeysPerJob
	redisRequeueScript := redis.NewScript(numKeys, redisLuaReenqueueJob)
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, jobType := range jobTypes {
		// pops from in progress, push into job queue and decrement the queue lock
		scriptArgs = append(scriptArgs, redisKeyJobsInProgress(r.namespace, poolID, jobType), redisKeyJobs(r.namespace, jobType), redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType)) // KEYS[1-4 * N]
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()

	// Keep moving jobs until all queues are empty
	for {
		values, err := redis.Values(redisRequeueScript.Do(conn, scriptArgs...))
		if err == redis.ErrNil {
			return nil
		} else if err != nil {
			return err
		}

		if len(values) != 3 {
			return fmt.Errorf("need 3 elements back")
		}
	}
}

// findDeadPools returns staled pools IDs and associated jobs.
func (r *deadPoolReaper) findDeadPools() (map[string][]string, error) {
	var scriptArgs []interface{} = []interface{}{
		redisKeyWorkerPools(r.namespace),
		r.deadTime.Seconds(),
		nowEpochSeconds(),
	}

	conn := r.pool.Get()
	defer conn.Close()

	dpools, err := redis.StringMap(redisTakeDeadPoolsScript.Do(conn, scriptArgs...))
	if err != nil {
		return nil, err
	}

	deadPools := make(map[string][]string, len(dpools))
	for k, v := range dpools {
		if v == "" {
			deadPools[k] = []string{}
		} else {
			deadPools[k] = strings.Split(v, ",")
		}
	}

	return deadPools, nil
}

// acquireLock acquires lock with a value and an expiration time for reap period.
func (r *deadPoolReaper) acquireLock(value string) (bool, error) {
	conn := r.pool.Get()
	defer conn.Close()

	reply, err := conn.Do(
		"SET", redisKeyReaperLock(r.namespace), value, "NX", "EX", int64(r.reapPeriod/time.Second))
	if err != nil {
		return false, err
	}

	return reply != nil, nil
}

// releaseLock releases lock with a value.
func (r *deadPoolReaper) releaseLock(value string) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := redisReleaseLockScript.Do(conn, redisKeyReaperLock(r.namespace), value)

	return err
}

func genValue() (string, error) {
	b := make([]byte, 16)

	_, err := crand.Read(b)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

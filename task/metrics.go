package task

import (
	"fmt"
	"strings"
	"time"

	"github.com/Guazi-inc/go-work"
	"github.com/go-errors/errors"
)

//redis keys
const (
	keyTasks        = "__all_tasks__"
	keyLock         = keyTasks + ":__lock_report__"
	hashKeySucc     = "success"
	hashKeyFail     = "fail"
	hashKeyElapsed  = "elapsed"
	hashKeyErr      = "last_error"
	hashKeyDesc     = "desc"
	hashKeyOwner    = "owner"
	hashKeySchedule = "schedule"
)

// ServicePrefix for LockKey name, should unique global, will be override when create worker_pool_manager
var ServicePrefix = ""

type jobMetrics struct {
	name     string
	ns       string
	owner    string
	schedule string
	succ     string
	fail     string
	elapsed  string
	lastErr  string
	desc     string
}

func saveMetaInfo(t *TaskBase, jobName string) {
	ns := t.GetNamespace()
	_ = redis.HSet(keyTask(), fieldKeyByName(ns, jobName, hashKeyOwner), t.Owner)
	_ = redis.HSet(keyTask(), fieldKeyByName(ns, jobName, hashKeyDesc), t.GetDesc())
	_ = redis.HSet(keyTask(), fieldKeyByName(ns, jobName, hashKeySchedule), t.GetSpecStr(jobName))
}

// saveMetrics called after exec the job whenever fail or success
func saveMetrics(t *TaskBase, j *work.Job, elapsed time.Duration, runErr error) {
	if redis == nil {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("[go-work error] saver Metrics err: ", err)
		}
	}()

	if runErr == nil {
		_, _ = redis.HIncrBy(keyTask(), fieldKey(t, j, hashKeyElapsed), elapsed.Nanoseconds()/int64(time.Millisecond))
		_ = redis.HIncr(keyTask(), fieldKey(t, j, hashKeySucc))
	}

}

func fetchMetrics() (map[string]*jobMetrics, error) {
	if redis == nil {
		return nil, errors.New("[go-work error] redis cache nil!")
	}
	// duplicate sending check, if prefix not specified, will skip lock check
	if ServicePrefix != "" {
		if rl, err := redis.SetNX(ServicePrefix+keyLock, fmt.Sprintf("%d", time.Now().UnixNano()/1e6), time.Hour*23); err == nil && !rl {
			return nil, nil
		}
	}

	res, err := redis.HGetAll(keyTask())
	if err != nil {
		return nil, err
	}
	_ = redis.Del(keyTask())

	jobs := make(map[string]*jobMetrics)
	for k, v := range res {
		l := strings.Split(k, ":")
		key := l[0] + l[1]
		js, ok := jobs[key]
		if !ok {
			js = &jobMetrics{
				name: l[1],
				ns:   l[0],
			}
		}

		switch l[2] {
		case hashKeySucc:
			js.succ = v
		case hashKeySchedule:
			js.schedule = v
		case hashKeyFail:
			js.fail = v
		case hashKeyOwner:
			js.owner = v
		case hashKeyElapsed:
			js.elapsed = v
		case hashKeyDesc:
			js.desc = v
		case hashKeyErr:
			js.lastErr = v
		}
		jobs[key] = js
	}
	return jobs, nil
}

func postMetrics(t *TaskBase, j *work.Job) {
	if len(j.LastErr) > 0 {
		_ = redis.HIncr(keyTask(), fieldKey(t, j, hashKeyFail))
		_ = redis.HSet(keyTask(), fieldKey(t, j, hashKeyErr), truncateString(j.LastErr, 500))
	}
}

func truncateString(str string, num int) string {
	bnoden := str
	if len(str) > num {
		if num > 3 {
			num -= 3
		}
		bnoden = str[0:num] + "..."
	}
	return bnoden
}

func fieldKeyByName(ns, jobName, field string) string {
	return fmt.Sprintf("%s:%s:%s", ns, jobName, field)
}

func fieldKey(t *TaskBase, j *work.Job, field string) string {
	return fieldKeyByName(t.GetNamespace(), j.Name, field)
}

func keyTask() string {
	return ServicePrefix + keyTasks
}

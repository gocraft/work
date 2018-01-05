package manager

import (
	"fmt"

	"os"
	"os/signal"
	"syscall"

	"strings"

	"regexp"

	"github.com/Guazi-inc/go-work"
	"github.com/Guazi-inc/go-work/task"
	"github.com/fatih/structs"
	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
	"golang.guazi-corp.com/znkf/data-soup/http/staff"
)

// WorkerPoolManager manager instance
type WorkerPoolManager struct {
	pools       map[string]*work.WorkerPool
	redisPool   *redis.Pool
	rootPrefix  string
	isProd      bool
	concurrency map[string]uint
}

// NewWorkerPoolManager create the pool manager instance, should be singleton
func NewWorkerPoolManager(prefix, redisEndpoint string, isProd bool, cache task.CronCache, sh *staff.HTTP) *WorkerPoolManager {
	if len(prefix) == 0 {
		return nil
	}

	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}

	redisPool := &redis.Pool{
		MaxActive: 10,
		MaxIdle:   10,

		Wait: true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisEndpoint)
		},
	}

	task.ServicePrefix = prefix
	task.SetCache(cache)
	task.SetStaffClient(sh)

	pools := make(map[string]*work.WorkerPool)
	return &WorkerPoolManager{
		pools:       pools,
		redisPool:   redisPool,
		rootPrefix:  prefix,
		isProd:      isProd,
		concurrency: make(map[string]uint),
	}
}

func (wpm *WorkerPoolManager) getConcurrency(ns string) uint {
	if c, ok := wpm.concurrency[ns]; ok {
		return c
	}
	return 1
}

func (wpm *WorkerPoolManager) SetConcurrency(m map[string]uint) {
	wpm.concurrency = m
}

func (wpm *WorkerPoolManager) SetMailGroup(l []string) {
	for _, receiver := range l {
		task.AddMailReceiver(receiver)
	}
}

// SetDailyReportTime 设置每天日报时间，格式为 "hh:mm:ss"
func (wpm *WorkerPoolManager) SetDailyReportTime(t string) error {
	f := `^(20|21|22|23|[0-1]\d):[0-5]\d:[0-5]\d$`
	r := regexp.MustCompile(f)
	if !r.Match([]byte(t)) {
		return errors.New("[go-work error] invalid DailyReportTime format, should be hh:mm:ss")
	}
	task.DailyReportTime = t
	return nil
}

func (wpm *WorkerPoolManager) SetDailyReportTitle(t string) {
	task.Title = t
}

//GetPool retrieve pool instance, create if not exist
func (wpm *WorkerPoolManager) GetPool(ctx interface{}) *work.WorkerPool {
	task := ctx.(task.Task)
	poolName := task.GetNamespace()
	if pool, ok := wpm.pools[poolName]; ok {
		return pool
	}

	threads := wpm.getConcurrency(poolName)

	pool := work.NewWorkerPool(ctx, threads, wpm.rootPrefix+task.GetNamespace(), wpm.redisPool)
	pool.Middleware(task.Log)
	pool.Middleware(task.Metrics)
	wpm.pools[poolName] = pool

	//ns := reflect.TypeOf(ctx).PkgPath()
	return pool
}

// Start start work
func (wpm *WorkerPoolManager) Start() {
	_ = wpm.RegisterPeriodicTask(&task.TasksDailyReport{})

	for _, pool := range wpm.pools {
		pool.Start()
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	wpm.Stop()
}

// Stop stop the pool
func (wpm *WorkerPoolManager) Stop() {
	fmt.Println("[go-work] graceful shutdown........")
	for _, pool := range wpm.pools {
		pool.Stop()
	}
}

//RegisterPeriodicTask register cron job to the worker pool, will create the pool if not exist
func (wpm *WorkerPoolManager) RegisterPeriodicTask(it interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.New(fmt.Sprintf("Register Task error:%v", e))
		}
	}()
	t := it.(task.Task)
	err = t.Setup()
	if err != nil {
		return err
	}

	taskName := structs.Name(t)
	if t.GetProductionOnly() && !wpm.isProd {
		fmt.Println(taskName, " not in production,  skipped ")
	}

	pool := wpm.GetPool(it)
	jobs := t.GetJobs()
	if jobs == nil {
		return errors.New(fmt.Sprintf("[go-work error] there is no job found for Task[%s] ", taskName))
	}

	for jobName, job := range jobs {
		spec := job.TimerSpec
		cronStr, err := spec.Spec()
		if err != nil {
			return errors.New(fmt.Sprintf("[go-work error] parse cron for job[%s] error: %s", jobName, err.Error()))
		}

		// add post handler for collecting error metrics
		opts := work.JobOptions{
			PostHandler: t.PostProcess,
			Spec:        cronStr,
		}
		pool.JobWithOptions(jobName, opts, job.Fun)
		pool.PeriodicallyEnqueue(cronStr, jobName)
		t.SaveInfo(jobName)
	}

	task.AddMailReceiver(t.GetOwner())

	return nil
}

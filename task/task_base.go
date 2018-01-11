package task

import (
	"errors"
	"time"

	"fmt"

	"github.com/Guazi-inc/go-work"
	"github.com/fatih/structs"
)

//Task Common Interface of the Task
type Task interface {

	// Setup the task including task specific config settings & resources, and schedule the jobs
	Setup() error

	// Exec job worker function, you must schedule it in the Setup() function
	Exec(job *work.Job) error

	SetMetaData(s Task, namespace, owner, description string, productionOnly bool)

	GetJobs() map[string]*MultiJobs
	PostProcess(job *work.Job)

	GetNamespace() string
	GetProductionOnly() bool
	GetSpecStr(jobName string) string
	GetOwner() string
	Log(job *work.Job, next work.NextMiddlewareFunc) error
	Metrics(job *work.Job, next work.NextMiddlewareFunc) error
	SaveInfo(jobName string)
}

// MultiJobs the struct for individual job of the task
type MultiJobs struct {
	Fun       func(job *work.Job) error
	TimerSpec *work.TimerSpec
}

// TaskBase the base structure for every task, should be embed in your task struct
type TaskBase struct {
	Jobs           map[string]*MultiJobs
	ProductionOnly bool

	self        Task
	Concurrency uint
	Namespace   string
	Owner       string // owner of the script
	Description string // the description of each job, please fill in 中文
}

func (t *TaskBase) SetMetaData(self Task, ns, owner, desc string, prodOnly bool) {
	t.self = self
	t.Owner = owner
	t.Namespace = ns
	t.Description = desc
	t.ProductionOnly = prodOnly
}

func (t *TaskBase) SaveInfo(jobName string) {
	saveMetaInfo(t, jobName)
}

// GetOwner get the owner of the job
func (t *TaskBase) GetOwner() string {
	return t.Owner
}

// GetDesc get the description or this task
func (t *TaskBase) GetDesc() string {
	return t.Description
}

// GetJobs get the jobs or this task
func (t *TaskBase) GetJobs() map[string]*MultiJobs {
	return t.Jobs
}

// GetProductionOnly if true,will not exec outside the production env, set t.ProductionOnly as needed
func (t *TaskBase) GetProductionOnly() bool {
	return t.ProductionOnly
}

// GetNamespace get the namespace for the job, use as prefix in redis
func (t *TaskBase) GetNamespace() string {
	return t.Namespace
}

// Log log job result
func (t *TaskBase) Log(job *work.Job, next work.NextMiddlewareFunc) error {
	st := time.Now()

	if err := next(); err != nil {
		return err
	}

	fmt.Printf("finish job [%s-%s], started at:%d, duration: %d ms \n",
		job.Name, job.ID, st.Unix(), (time.Now().UnixNano()-st.UnixNano())/1e6)
	return nil
}

// Metrics collect job run metrics for daily report, should be the last middleware
func (t *TaskBase) Metrics(job *work.Job, next work.NextMiddlewareFunc) error {
	st := time.Now()

	err := next()
	if err != nil {
		fmt.Printf("[go-work error] [%s],started at:%d, return err: %s \n", job.Name, st.Unix(), err.Error())
	}

	saveMetrics(t, job, time.Since(st), err)
	return err
}

// PostProcess save metrics
func (t *TaskBase) PostProcess(j *work.Job) {
	postMetrics(t, j)
}

// AddCronTask add task to worker
func (t *TaskBase) AddCronTask(subName string, spec *work.TimerSpec, f func(job *work.Job) error) error {
	if t.Jobs == nil {
		t.Jobs = make(map[string]*MultiJobs)
	}

	jobName := structs.Name(t.self) + subName
	if _, ok := t.Jobs[jobName]; ok {
		return errors.New(fmt.Sprintf("register duplicated job name:%s ", jobName))
	}

	t.Jobs[jobName] = &MultiJobs{
		Fun:       f,
		TimerSpec: spec,
	}
	return nil
}

// GetSpecStr get the spec string for metrics
func (t *TaskBase) GetSpecStr(jobName string) string {
	if t.Jobs == nil {
		return ""
	}

	if v, ok := t.Jobs[jobName]; ok {
		str, _ := v.TimerSpec.Spec()
		return str
	}
	return ""
}

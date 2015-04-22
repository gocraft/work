package work

import (
	"encoding/json"
	"reflect"
)

type Job struct {
	// Inputs when makin a new job
	Name       string        `json:"name,omitempty"`
	ID         string        `json:"id"`
	EnqueuedAt int64         `json:"t"`
	Args       []interface{} `json:"args"`

	// Inputs when retrying
	Fails    int64  `json:"fails,omitempty"` // number of times this job has failed
	LastErr  string `json:"err,omitempty"`
	FailedAt int64  `json:"failed_at,omitempty"`

	rawJSON      []byte
	dequeuedFrom []byte
	inProgQueue  []byte
}

type jobType struct {
	Name string
	JobOptions

	IsGeneric      bool
	GenericHandler GenericHandler
	DynamicHandler reflect.Value
}

type JobOptions struct {
	Priority uint
	MaxFails uint // 0: send straight to dead (unless SkipDead)
	SkipDead bool //
}

type GenericHandler func(*Job) error

func newJob(rawJSON, dequeuedFrom, inProgQueue []byte) (*Job, error) {
	var job Job
	err := json.Unmarshal(rawJSON, &job)
	if err != nil {
		return nil, err
	}
	job.rawJSON = rawJSON
	job.dequeuedFrom = dequeuedFrom
	job.inProgQueue = inProgQueue
	return &job, nil
}

func newJobTypeGeneric(name string, opts JobOptions, handler GenericHandler) *jobType {
	return &jobType{
		Name:           name,
		JobOptions:     opts,
		IsGeneric:      true,
		GenericHandler: handler,
	}
}

func (j *Job) Serialize() ([]byte, error) {
	return json.Marshal(j)
}

func (j *Job) failed(err error) {
	j.Fails++ // todo: factor into job.failed(runErr)
	j.LastErr = err.Error()
	j.FailedAt = nowEpochSeconds()
}

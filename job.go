package work

import (
	"encoding/json"
)

type Job struct {
	// Inputs when makin a new job
	Name       string                 `json:"name,omitempty"`
	ID         string                 `json:"id"`
	EnqueuedAt int64                  `json:"t"`
	Args       map[string]interface{} `json:"args"`

	// Inputs when retrying
	Fails    int64  `json:"fails,omitempty"` // number of times this job has failed
	LastErr  string `json:"err,omitempty"`
	FailedAt int64  `json:"failed_at,omitempty"`

	rawJSON      []byte
	dequeuedFrom []byte
	inProgQueue  []byte
}

type Q map[string]interface{}

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

func (j *Job) Serialize() ([]byte, error) {
	return json.Marshal(j)
}

func (j *Job) SetArg(key string, val interface{}) {
	if j.Args == nil {
		j.Args = make(map[string]interface{})
	}
	j.Args[key] = val
}

func (j *Job) failed(err error) {
	j.Fails++ // todo: factor into job.failed(runErr)
	j.LastErr = err.Error()
	j.FailedAt = nowEpochSeconds()
}

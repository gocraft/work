package work

import (
	"encoding/json"
)

// Job represents a job.
type Job struct {
	// Inputs when making a new job
	Name       string `json:"name,omitempty"`
	ID         string `json:"id"`
	EnqueuedAt int64  `json:"t"`

	// Payload can be any json-marshallable object. This leads to a
	// double json-marshal which could be a bit better optimized,
	// but makes it easier to have custom job definitions.
	Payload []byte `json:"payload"`

	Unique bool `json:"unique,omitempty"`

	// Inputs when retrying
	Fails    int64  `json:"fails,omitempty"` // number of times this job has failed
	LastErr  string `json:"err,omitempty"`
	FailedAt int64  `json:"failed_at,omitempty"`

	rawJSON      []byte
	dequeuedFrom []byte
	inProgQueue  []byte
	argError     error
	observer     *observer
}

func (j *Job) SetPayload(payload interface{}) error {
	marshaled, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	j.Payload = marshaled
	return nil
}

func (j *Job) UnmarshalPayload(dest interface{}) error {
	if len(j.Payload) == 0 {
		return nil
	}

	return json.Unmarshal(j.Payload, dest)
}

// Q is a shortcut to easily specify arguments for jobs when enqueueing them.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com", "track": true})
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

func (j *Job) serialize() ([]byte, error) {
	return json.Marshal(j)
}

func (j *Job) failed(err error) {
	j.Fails++
	j.LastErr = err.Error()
	j.FailedAt = nowEpochSeconds()
}

// Checkin will update the status of the executing job to the specified messages. This message is visible within the web UI. This is useful for indicating some sort of progress on very long running jobs. For instance, on a job that has to process a million records over the course of an hour, the job could call Checkin with the current job number every 10k jobs.
func (j *Job) Checkin(msg string) {
	if j.observer != nil {
		j.observer.observeCheckin(j.Name, j.ID, msg)
	}
}

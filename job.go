package work

import (
	"encoding/json"
)

type Job struct {
	Name       string        `json:"name,omitempty"`
	ID         string        `json:"id"`
	SourceID   int64         `json:"source,omitempty"`
	EnqueuedAt int64         `json:"t"`
	Args       []interface{} `json:"args"`
	
	
	dequeuedFrom []byte
}

func newJob(jsonBytes, dequeuedFrom []byte) (*Job, error) {
	var job Job
	err := json.Unmarshal(jsonBytes, &job)
	if err != nil {
		return nil, err
	}
	job.dequeuedFrom = dequeuedFrom
	return &job, nil
}

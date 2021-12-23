// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"github.com/garyburd/redigo/redis"
)

// Status represents the different statuses a job can have.
type Status string

const (
	// StatusSaved is the status of any job that has been saved into the database but not yet queued
	StatusSaved Status = "saved"
	// StatusQueued is the status of any job that has been queued for execution but not yet selected
	StatusQueued Status = "queued"
	// StatusExecuting is the status of any job that has been selected for execution and is being delegated
	// to some worker and any job that is currently being executed by some worker.
	StatusExecuting Status = "executing"
	// StatusFinished is the status of any job that has been successfully executed.
	StatusFinished Status = "finished"
	// StatusFailed is the status of any job that failed to execute and for which there are no remaining retries.
	StatusFailed Status = "failed"
	// StatusCancelled is the status of any job that was manually cancelled.
	StatusCancelled Status = "cancelled"
	// StatusDestroyed is the status of any job that has been destroyed, i.e. completely removed
	// from the database.
	StatusDestroyed Status = "destroyed"
)

// key returns the key used for the sorted set in redis which will hold
// all jobs with this status.
func (status Status) Key() string {
	return "jobs:" + string(status)
}

// Count returns the number of jobs that currently have the given status
// or an error if there was a problem connecting to the database.
func (status Status) Count() (int, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("ZCARD", status.Key()))
}

// JobIds returns the ids of all jobs that have the given status, ordered by
// priority or an error if there was a problem connecting to the database.
func (status Status) JobIds() ([]string, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("ZREVRANGE", status.Key(), 0, -1))
}

// Jobs returns all jobs that have the given status, ordered by priority or
// an error if there was a problem connecting to the database.
func (status Status) Jobs() ([]*Job, error) {
	t := newTransaction()
	jobs := []*Job{}
	t.getJobsByIds(status.Key(), newScanJobsHandler(&jobs))
	if err := t.exec(); err != nil {
		return nil, err
	}
	return jobs, nil
}

// possibleStatuses is simply an array of all the possible job statuses.
var possibleStatuses = []Status{
	StatusSaved,
	StatusQueued,
	StatusExecuting,
	StatusFinished,
	StatusFailed,
	StatusCancelled,
	StatusDestroyed,
}

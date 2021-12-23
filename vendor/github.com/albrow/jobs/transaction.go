// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

// transaction is an abstraction layer around a redis transaction.
// transactions feature delayed execution, so nothing touches the database
// until exec is called.
type transaction struct {
	conn    redis.Conn
	actions []*action
}

// action is a single step in a transaction and must be either a command
// or a script with optional arguments.
type action struct {
	kind    actionKind
	name    string
	script  *redis.Script
	args    redis.Args
	handler replyHandler
}

// actionKind is either a command or a script
type actionKind int

const (
	actionCommand = iota
	actionScript
)

// replyHandler is a function which does something with the reply from a redis
// command or script.
type replyHandler func(interface{}) error

// newTransaction instantiates and returns a new transaction.
func newTransaction() *transaction {
	t := &transaction{
		conn: redisPool.Get(),
	}
	return t
}

// command adds a command action to the transaction with the given args.
// handler will be called with the reply from this specific command when
// the transaction is executed.
func (t *transaction) command(name string, args redis.Args, handler replyHandler) {
	t.actions = append(t.actions, &action{
		kind:    actionCommand,
		name:    name,
		args:    args,
		handler: handler,
	})
}

// command adds a script action to the transaction with the given args.
// handler will be called with the reply from this specific script when
// the transaction is executed.
func (t *transaction) script(script *redis.Script, args redis.Args, handler replyHandler) {
	t.actions = append(t.actions, &action{
		kind:    actionScript,
		script:  script,
		args:    args,
		handler: handler,
	})
}

// sendAction writes a to a connection buffer using conn.Send()
func (t *transaction) sendAction(a *action) error {
	switch a.kind {
	case actionCommand:
		return t.conn.Send(a.name, a.args...)
	case actionScript:
		return a.script.Send(t.conn, a.args...)
	}
	return nil
}

// doAction writes a to the connection buffer and then immediately
// flushes the buffer and reads the reply via conn.Do()
func (t *transaction) doAction(a *action) (interface{}, error) {
	switch a.kind {
	case actionCommand:
		return t.conn.Do(a.name, a.args...)
	case actionScript:
		return a.script.Do(t.conn, a.args...)
	}
	return nil, nil
}

// exec executes the transaction, sequentially sending each action and
// calling all the action handlers with the corresponding replies.
func (t *transaction) exec() error {
	// Return the connection to the pool when we are done
	defer t.conn.Close()

	if len(t.actions) == 1 {
		// If there is only one command, no need to use MULTI/EXEC
		a := t.actions[0]
		reply, err := t.doAction(a)
		if err != nil {
			return err
		}
		if a.handler != nil {
			if err := a.handler(reply); err != nil {
				return err
			}
		}
	} else {
		// Send all the commands and scripts at once using MULTI/EXEC
		t.conn.Send("MULTI")
		for _, a := range t.actions {
			if err := t.sendAction(a); err != nil {
				return err
			}
		}

		// Invoke redis driver to execute the transaction
		replies, err := redis.Values(t.conn.Do("EXEC"))
		if err != nil {
			return err
		}

		// Iterate through the replies, calling the corresponding handler functions
		for i, reply := range replies {
			a := t.actions[i]
			if a.handler != nil {
				if err := a.handler(reply); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// newScanJobHandler returns a replyHandler which, when run, will scan the values
// of reply into job.
func newScanJobHandler(job *Job) replyHandler {
	return func(reply interface{}) error {
		return scanJob(reply, job)
	}
}

// newScanJobsHandler returns a replyHandler which, when run, will scan the values
// of reply into jobs.
func newScanJobsHandler(jobs *[]*Job) replyHandler {
	return func(reply interface{}) error {
		values, err := redis.Values(reply, nil)
		if err != nil {
			return nil
		}
		for _, fields := range values {
			job := &Job{}
			if err := scanJob(fields, job); err != nil {
				return err
			}
			(*jobs) = append((*jobs), job)
		}
		return nil
	}
}

// debugSet simply prints out the value of the given set
func (t *transaction) debugSet(setName string) {
	t.command("ZRANGE", redis.Args{setName, 0, -1, "WITHSCORES"}, func(reply interface{}) error {
		vals, err := redis.Strings(reply, nil)
		if err != nil {
			return err
		}
		fmt.Printf("%s: %v\n", setName, vals)
		return nil
	})
}

// newScanStringsHandler returns a replyHandler which, when run, will scan the values
// of reply into strings.
func newScanStringsHandler(strings *[]string) replyHandler {
	return func(reply interface{}) error {
		if strings == nil {
			return fmt.Errorf("jobs: Error in newScanStringsHandler: expected strings arg to be a pointer to a slice of strings but it was nil")
		}
		var err error
		(*strings), err = redis.Strings(reply, nil)
		if err != nil {
			return fmt.Errorf("jobs: Error in newScanStringsHandler: %s", err.Error())
		}
		return nil
	}
}

// newScanStringHandler returns a replyHandler which, when run, will convert reply to a
// string and scan it into s.
func newScanStringHandler(s *string) replyHandler {
	return func(reply interface{}) error {
		if s == nil {
			return fmt.Errorf("jobs: Error in newScanStringHandler: expected arg s to be a pointer to a string but it was nil")
		}
		var err error
		(*s), err = redis.String(reply, nil)
		if err != nil {
			return fmt.Errorf("jobs: Error in newScanStringHandler: %s", err.Error())
		}
		return nil
	}
}

// newScanIntHandler returns a replyHandler which, when run, will convert reply to a
// int and scan it into i.
func newScanIntHandler(i *int) replyHandler {
	return func(reply interface{}) error {
		if i == nil {
			return fmt.Errorf("jobs: Error in newScanIntHandler: expected arg s to be a pointer to a string but it was nil")
		}
		var err error
		(*i), err = redis.Int(reply, nil)
		if err != nil {
			return fmt.Errorf("jobs: Error in newScanIntHandler: %s", err.Error())
		}
		return nil
	}
}

// newScanBoolHandler returns a replyHandler which, when run, will convert reply to a
// bool and scan it into b.
func newScanBoolHandler(b *bool) replyHandler {
	return func(reply interface{}) error {
		if b == nil {
			return fmt.Errorf("jobs: Error in newScanBoolHandler: expected arg v to be a pointer to a bool but it was nil")
		}
		var err error
		(*b), err = redis.Bool(reply, nil)
		if err != nil {
			return fmt.Errorf("jobs: Error in newScanBoolHandler: %s", err.Error())
		}
		return nil
	}
}

//go:generate go run scripts/generate.go

// popNextJobs is a small function wrapper around getAndMovesJobToExecutingScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will get the next n jobs from the queue that are ready based on their time parameter.
func (t *transaction) popNextJobs(n int, poolId string, handler replyHandler) {
	currentTime := time.Now().UTC().UnixNano()
	t.script(popNextJobsScript, redis.Args{n, currentTime, poolId}, handler)
}

// retryOrFailJob is a small function wrapper around retryOrFailJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will either mark the job as failed or queue it for retry depending on the number of
// retries left.
func (t *transaction) retryOrFailJob(job *Job, handler replyHandler) {
	t.script(retryOrFailJobScript, redis.Args{job.id}, handler)
}

// setStatus is a small function wrapper around setStatusScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will atomically update the status of the job, removing it from its old status set and
// adding it to the new one.
func (t *transaction) setStatus(job *Job, status Status) {
	t.script(setJobStatusScript, redis.Args{job.id, string(status)}, nil)
}

// destroyJob is a small function wrapper around destroyJobScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will remove all records associated with job from the database.
func (t *transaction) destroyJob(job *Job) {
	t.script(destroyJobScript, redis.Args{job.id}, nil)
}

// purgeStalePool is a small function wrapper around purgeStalePoolScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will remove the stale pool from the active pools set, and then requeue any jobs associated
// with the stale pool that are stuck in the executing set.
func (t *transaction) purgeStalePool(poolId string) {
	t.script(purgeStalePoolScript, redis.Args{poolId}, nil)
}

// getJobsByIds is a small function wrapper around getJobsByIdsScript.
// It offers some type safety and helps make sure the arguments you pass through to the are correct.
// The script will return all the fields for jobs which are identified by ids in the given sorted set.
// You can use the handler to scan the jobs into a slice of jobs.
func (t *transaction) getJobsByIds(setKey string, handler replyHandler) {
	t.script(getJobsByIdsScript, redis.Args{setKey}, handler)
}

// setJobField is a small function wrapper around setJobFieldScript.
// It offers some type safety and helps make sure the arguments you pass through are correct.
// The script will set the given field to the given value iff the job exists and has not been
// destroyed.
func (t *transaction) setJobField(job *Job, fieldName string, fieldValue interface{}) {
	t.script(setJobFieldScript, redis.Args{job.id, fieldName, fieldValue}, nil)
}

// addJobToSet is a small function wrapper around addJobToSetScript.
// It offers some type safety and helps make sure the arguments you pass through are correct.
// The script will add the job to the given set with the given score iff the job exists
// and has not been destroyed.
func (t *transaction) addJobToSet(job *Job, setName string, score float64) {
	t.script(addJobToSetScript, redis.Args{job.id, setName, score}, nil)
}

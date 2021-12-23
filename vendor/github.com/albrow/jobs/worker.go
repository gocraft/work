// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

// worker continuously executes jobs within its own goroutine.
// The jobs chan is shared between all jobs. To stop the worker,
// simply close the jobs channel.
type worker struct {
	jobs      chan *Job
	wg        *sync.WaitGroup
	afterFunc func(*Job)
}

// start starts a goroutine in which the worker will continuously
// execute jobs until the jobs channel is closed.
func (w *worker) start() {
	go func() {
		for job := range w.jobs {
			w.doJob(job)
		}
		w.wg.Done()
	}()
}

// doJob executes the given job. It also sets the status and timestamps for
// the job appropriately depending on the outcome of the execution.
func (w *worker) doJob(job *Job) {
	if w.afterFunc != nil {
		defer w.afterFunc(job)
	}

	defer func() {
		if r := recover(); r != nil {
			// Get a reasonable error message from the panic
			msg := ""
			if err, ok := r.(error); ok {
				msg = err.Error()
			} else {
				msg = fmt.Sprint(r)
			}
			if err := setJobError(job, msg); err != nil {
				// Nothing left to do but panic
				panic(err)
			}
		}
	}()
	// Set the started field and save the job
	job.started = time.Now().UTC().UnixNano()
	t0 := newTransaction()
	t0.setJobField(job, "started", job.started)
	if err := t0.exec(); err != nil {
		if err := setJobError(job, err.Error()); err != nil {
			// NOTE: panics will be caught by the recover statment above
			panic(err)
		}
		return
	}
	// Use reflection to instantiate arguments for the handler
	handlerArgs := []reflect.Value{}
	if job.typ.dataType != nil {
		// Instantiate a new variable to hold this argument
		dataVal := reflect.New(job.typ.dataType)
		if err := decode(job.data, dataVal.Interface()); err != nil {
			if err := setJobError(job, err.Error()); err != nil {
				// NOTE: panics will be caught by the recover statment above
				panic(err)
			}
			return
		}
		handlerArgs = append(handlerArgs, dataVal.Elem())
	}
	// Call the handler using the arguments we just instantiated
	handlerVal := reflect.ValueOf(job.typ.handler)
	returnVals := handlerVal.Call(handlerArgs)
	// Set the finished timestamp
	job.finished = time.Now().UTC().UnixNano()

	// Check if the error return value was nil
	if !returnVals[0].IsNil() {
		err := returnVals[0].Interface().(error)
		if err := setJobError(job, err.Error()); err != nil {
			// NOTE: panics will be caught by the recover statment above
			panic(err)
		}
		return
	}
	t1 := newTransaction()
	t1.setJobField(job, "finished", job.finished)
	if job.IsRecurring() {
		// If the job is recurring, reschedule and set status to queued
		job.time = job.NextTime()
		t1.setJobField(job, "time", job.time)
		t1.addJobToTimeIndex(job)
		t1.setStatus(job, StatusQueued)
	} else {
		// Otherwise, set status to finished
		t1.setStatus(job, StatusFinished)
	}
	if err := t1.exec(); err != nil {
		if err := setJobError(job, err.Error()); err != nil {
			// NOTE: panics will be caught by the recover statment above
			panic(err)
		}
		return
	}
}

func setJobError(job *Job, msg string) error {
	// Start a new transaction
	t := newTransaction()
	// Set the job error field
	t.setJobField(job, "error", msg)
	// Either queue the job for retry or mark it as failed depending
	// on how many retries the job has left
	t.retryOrFailJob(job, nil)
	if err := t.exec(); err != nil {
		return err
	}
	return nil
}

// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"fmt"
	"reflect"
	"time"
)

// Types is map of job type names to *Type
var Types = map[string]*Type{}

// Type represents a type of job that can be executed by workers
type Type struct {
	name     string
	handler  interface{}
	retries  uint
	dataType reflect.Type
}

// ErrorNameAlreadyRegistered is returned whenever RegisterType is called
// with a name that has already been registered.
type ErrorNameAlreadyRegistered struct {
	name string
}

// Error satisfies the error interface.
func (e ErrorNameAlreadyRegistered) Error() string {
	return fmt.Sprintf("jobs: Cannot register job type because job type with name %s already exists", e.name)
}

// newErrorNameAlreadyRegistered returns an ErrorNameAlreadyRegistered with the given name.
func newErrorNameAlreadyRegistered(name string) ErrorNameAlreadyRegistered {
	return ErrorNameAlreadyRegistered{name: name}
}

// A HandlerFunc is a function which accepts ether zero or one arguments and returns an error.
// The function will be executed by a worker. If the function returns a non-nil error or causes
// a panic, the worker will capture and log the error, and if applicable the job may be queued
// for retry.
type HandlerFunc interface{}

// RegisterType registers a new type of job that can be executed by workers.
// name should be a unique string identifier for the job.
// retries is the number of times this type of job should be retried if it fails.
// handler is a function that a worker will call in order to execute the job.
// handler should be a function which accepts either 0 or 1 arguments of any type,
// corresponding to the data for a job of this type. All jobs of this type must have
// data with the same type as the first argument to handler, or nil if the handler
// accepts no arguments.
func RegisterType(name string, retries uint, handler HandlerFunc) (*Type, error) {
	// Make sure name is unique
	if _, found := Types[name]; found {
		return Types[name], newErrorNameAlreadyRegistered(name)
	}
	// Make sure handler is a function
	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
		return nil, fmt.Errorf("jobs: in RegisterNewType, handler must be a function. Got %T", handler)
	}
	if handlerType.NumIn() > 1 {
		return nil, fmt.Errorf("jobs: in RegisterNewType, handler must accept 0 or 1 arguments. Got %d.", handlerType.NumIn())
	}
	if handlerType.NumOut() != 1 {
		return nil, fmt.Errorf("jobs: in RegisterNewType, handler must have exactly one return value. Got %d.", handlerType.NumOut())
	}
	if !typeIsError(handlerType.Out(0)) {
		return nil, fmt.Errorf("jobs: in RegisterNewType, handler must return an error. Got return value of type %s.", handlerType.Out(0).String())
	}
	Type := &Type{
		name:    name,
		handler: handler,
		retries: retries,
	}
	if handlerType.NumIn() == 1 {
		Type.dataType = handlerType.In(0)
	}
	Types[name] = Type
	return Type, nil
}

var errorType = reflect.TypeOf(make([]error, 1)).Elem()

func typeIsError(typ reflect.Type) bool {
	return typ.Implements(errorType)
}

// String satisfies the Stringer interface and returns the name of the Type.
func (jt *Type) String() string {
	return jt.name
}

// Schedule schedules a on-off job of the given type with the given parameters.
// Jobs with a higher priority will be executed first. The job will not be
// executed until after time. data is the data associated with this particular
// job and should have the same type as the first argument to the handler for this
// Type.
func (jt *Type) Schedule(priority int, time time.Time, data interface{}) (*Job, error) {
	// Encode the data
	encodedData, err := jt.encodeData(data)
	if err != nil {
		return nil, err
	}
	// Create and save the job
	job := &Job{
		data:     encodedData,
		typ:      jt,
		time:     time.UTC().UnixNano(),
		retries:  jt.retries,
		priority: priority,
	}
	// Set the job's status to queued and save it in the database
	job.status = StatusQueued
	if err := job.save(); err != nil {
		return nil, err
	}
	return job, nil
}

// ScheduleRecurring schedules a recurring job of the given type with the given parameters.
// Jobs with a higher priority will be executed first. The job will not be executed until after
// time. After time, the job will be executed with a frequency specified by freq. data is the
// data associated with this particular job and should have the same type as the first argument
// to the handler for this Type. Every recurring execution of the job will use the
// same data.
func (jt *Type) ScheduleRecurring(priority int, time time.Time, freq time.Duration, data interface{}) (*Job, error) {
	// Encode the data
	encodedData, err := jt.encodeData(data)
	if err != nil {
		return nil, err
	}
	// Create and save the job
	job := &Job{
		data:     encodedData,
		typ:      jt,
		time:     time.UTC().UnixNano(),
		retries:  jt.retries,
		freq:     freq.Nanoseconds(),
		priority: priority,
	}
	// Set the job's status to queued and save it in the database
	job.status = StatusQueued
	if err := job.save(); err != nil {
		return nil, err
	}
	return job, nil
}

// encodeData checks that the type of data is what we expect based on the handler for the Type.
// If it is, it encodes the data into a slice of bytes.
func (jt *Type) encodeData(data interface{}) ([]byte, error) {
	// Check the type of data
	dataType := reflect.TypeOf(data)
	if dataType != jt.dataType {
		return nil, fmt.Errorf("jobs: provided data was not of the correct type.\nExpected %s for Type %s, but got %s", jt.dataType, jt, dataType)
	}
	// Encode the data
	encodedData, err := encode(data)
	if err != nil {
		return nil, fmt.Errorf("jobs: error encoding data: %s", err.Error())
	}
	return encodedData, nil
}

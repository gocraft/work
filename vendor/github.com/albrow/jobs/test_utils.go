// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

// setUpOnce enforces that certain pieces of the set up process only occur once,
// even after successive calls to testingSetUp.
var setUpOnce = sync.Once{}

// testingSetUp should be called at the beginning of any test that touches the database
// or registers new job types.
func testingSetUp() {
	setUpOnce.Do(func() {
		// Use database 14 and a unix socket connection for testing
		// TODO: allow this to be configured via command-line flags
		Config.Db.Database = 14
	})
	// Clear out any old job types
	Types = map[string]*Type{}
}

// testingSetUp should be called at the end of any test that touches the database
// or registers new job types, usually via defer.
func testingTeardown() {
	// Flush the database
	flushdb()
}

// flushdb removes all keys from the current database.
func flushdb() {
	conn := redisPool.Get()
	if _, err := conn.Do("FLUSHDB"); err != nil {
		panic(err)
	}
}

// noOpHandler is a HandlerFunc that simply does nothing
var noOpHandler = func() error { return nil }

// createTestJob creates and returns a job that can be used for testing.
func createTestJob() (*Job, error) {
	// Register the "testType"
	TypeName := "testType"
	Type, err := RegisterType(TypeName, 0, noOpHandler)
	if err != nil {
		if _, ok := err.(ErrorNameAlreadyRegistered); !ok {
			// If the name was already registered, that's fine.
			// We should return any other type of error
			return nil, err
		}
	}
	// Create and return a test job
	j := &Job{
		id:       "testJob",
		data:     []byte("testData"),
		typ:      Type,
		time:     time.Now().UTC().UnixNano(),
		priority: 100,
	}
	return j, nil
}

// createTestJobs creates and returns n jobs that can be used for testing.
// Each job has a unique id and priority, and the jobs are returned in order
// of decreasing priority.
func createTestJobs(n int) ([]*Job, error) {
	// Register the "testType"
	TypeName := "testType"
	Type, err := RegisterType(TypeName, 0, noOpHandler)
	if err != nil {
		if _, ok := err.(ErrorNameAlreadyRegistered); !ok {
			// If the name was already registered, that's fine.
			// We should return any other type of error
			return nil, err
		}
	}
	jobs := make([]*Job, n)
	for i := 0; i < n; i++ {
		jobs[i] = &Job{
			id:       fmt.Sprintf("testJob%d", i),
			data:     []byte("testData"),
			typ:      Type,
			time:     time.Now().UTC().UnixNano(),
			priority: (n - i) + 1,
		}
	}
	return jobs, nil
}

// createAndSaveTestJob creates, saves, and returns a job which can be used
// for testing. Each job has a unique id and priority, and the jobs are
// returned in order of decreasing priority.
func createAndSaveTestJob() (*Job, error) {
	j, err := createTestJob()
	if err != nil {
		return nil, err
	}
	if err := j.save(); err != nil {
		return nil, fmt.Errorf("Unexpected error in j.save(): %s", err.Error())
	}
	return j, nil
}

// createAndSaveTestJobs creates, saves, and returns n jobs which can be used
// for testing.
func createAndSaveTestJobs(n int) ([]*Job, error) {
	jobs, err := createTestJobs(n)
	if err != nil {
		return nil, err
	}
	// Save all the jobs in a single transaction
	t := newTransaction()
	for _, job := range jobs {
		t.saveJob(job)
	}
	if err := t.exec(); err != nil {
		return nil, err
	}
	return jobs, nil
}

// expectJobFieldEquals sets an error via t.Errorf if the the field identified by fieldName does
// not equal expected according to the database.
func expectJobFieldEquals(t *testing.T, job *Job, fieldName string, expected interface{}, converter replyConverter) {
	conn := redisPool.Get()
	defer conn.Close()
	got, err := conn.Do("HGET", job.Key(), fieldName)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if converter != nil {
		got, err = converter(got)
		if err != nil {
			t.Errorf("Unexpected error in converter: %s", err.Error())
		}
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("job.%s was not saved correctly.\n\tExpected: %v\n\tBut got:  %v", fieldName, expected, got)
	}
}

// replyConverter represents a function which is capable of converting a redis
// reply to some other type.
type replyConverter func(interface{}) (interface{}, error)

var (
	int64Converter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.Int64(in, nil)
	}
	intConverter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.Int(in, nil)
	}
	uintConverter replyConverter = func(in interface{}) (interface{}, error) {
		v, err := redis.Uint64(in, nil)
		if err != nil {
			return nil, err
		}
		return uint(v), nil
	}
	stringConverter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.String(in, nil)
	}
	bytesConverter replyConverter = func(in interface{}) (interface{}, error) {
		return redis.Bytes(in, nil)
	}
)

// expectSetContains sets an error via t.Errorf if member is not in the set
func expectSetContains(t *testing.T, setName string, member string) {
	conn := redisPool.Get()
	defer conn.Close()
	contains, err := redis.Bool(conn.Do("SISMEMBER", setName, member))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if !contains {
		t.Errorf("Expected set %s to contain %s but it did not.", setName, member)
	}
}

// expectSetDoesNotContain sets an error via t.Errorf if member is in the set
func expectSetDoesNotContain(t *testing.T, setName string, member string) {
	conn := redisPool.Get()
	defer conn.Close()
	contains, err := redis.Bool(conn.Do("SISMEMBER", setName, member))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if contains {
		t.Errorf("Expected set %s to not contain %s but it did.", setName, member)
	}
}

// expectJobInStatusSet sets an error via t.Errorf if job is not in the status set
// corresponding to status.
func expectJobInStatusSet(t *testing.T, j *Job, status Status) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", status.Key(), j.priority, j.priority))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job we were looking for
			return
		}
	}
	// If we reached here, we did not find the job we were looking for
	t.Errorf("job:%s was not found in set %s", j.id, status.Key())
}

// expectJobInTimeIndex sets an error via t.Errorf if job is not in the time index
// set.
func expectJobInTimeIndex(t *testing.T, j *Job) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", Keys.JobsTimeIndex, j.time, j.time))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job we were looking for
			return
		}
	}
	// If we reached here, we did not find the job we were looking for
	t.Errorf("job:%s was not found in set %s", j.id, Keys.JobsTimeIndex)
}

// expectJobNotInStatusSet sets an error via t.Errorf if job is in the status set
// corresponding to status.
func expectJobNotInStatusSet(t *testing.T, j *Job, status Status) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", status.Key(), j.priority, j.priority))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job, but it wasn't supposed to be here!
			t.Errorf("job:%s was found in set %s but expected it to be removed", j.id, status.Key())
		}
	}
}

// expectJobNotInTimeIndex sets an error via t.Errorf if job is in the time index
// set.
func expectJobNotInTimeIndex(t *testing.T, j *Job) {
	conn := redisPool.Get()
	defer conn.Close()
	gotIds, err := redis.Strings(conn.Do("ZRANGEBYSCORE", Keys.JobsTimeIndex, j.time, j.time))
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	for _, id := range gotIds {
		if id == j.id {
			// We found the job, but it wasn't supposed to be here!
			t.Errorf("job:%s was found in set %s but expected it to be removed", j.id, Keys.JobsTimeIndex)
		}
	}
}

// expectStatusEquals sets an error via t.Errorf if job.status does not equal expected,
// if the status field for the job in the database does not equal expected, if the job is
// not in the status set corresponding to expected, or if the job is in some other status
// set.
func expectStatusEquals(t *testing.T, job *Job, expected Status) {
	if job.status != expected {
		t.Errorf("Expected jobs:%s status to be %s but got %s", job.id, expected, job.status)
	}
	if expected == StatusDestroyed {
		// If the status is destroyed, we don't expect the job to be in the database
		// anymore.
		expectJobDestroyed(t, job)
	} else {
		// For every status other status, we expect the job to be in the database
		for _, status := range possibleStatuses {
			if status == expected {
				// Make sure the job hash has the correct status
				expectJobInStatusSet(t, job, status)
				// Make sure the job is in the correct set
				expectJobFieldEquals(t, job, "status", string(status), stringConverter)
			} else {
				// Make sure the job is not in any other set
				expectJobNotInStatusSet(t, job, status)
			}
		}
	}
}

// expectJobDestroyed sets an error via t.Errorf if job has not been destroyed.
func expectJobDestroyed(t *testing.T, job *Job) {
	// Make sure the main hash is gone
	expectKeyNotExists(t, job.Key())
	expectJobNotInTimeIndex(t, job)
	for _, status := range possibleStatuses {
		expectJobNotInStatusSet(t, job, status)
	}
}

// expectKeyExists sets an error via t.Errorf if key does not exist in the database.
func expectKeyExists(t *testing.T, key string) {
	conn := redisPool.Get()
	defer conn.Close()
	if exists, err := redis.Bool(conn.Do("EXISTS", key)); err != nil {
		t.Errorf("Unexpected error in EXISTS: %s", err.Error())
	} else if !exists {
		t.Errorf("Expected key %s to exist, but it did not.", key)
	}
}

// expectKeyNotExists sets an error via t.Errorf if key does exist in the database.
func expectKeyNotExists(t *testing.T, key string) {
	conn := redisPool.Get()
	defer conn.Close()
	if exists, err := redis.Bool(conn.Do("EXISTS", key)); err != nil {
		t.Errorf("Unexpected error in EXISTS: %s", err.Error())
	} else if exists {
		t.Errorf("Expected key %s to not exist, but it did exist.", key)
	}
}

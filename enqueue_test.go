package work

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueue(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)
	job, err := enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.Equal(t, "wat", job.Name)
	assert.True(t, len(job.ID) > 10)                        // Something is in it
	assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", job.ArgString("b"))
	assert.EqualValues(t, 1, job.ArgInt64("a"))
	assert.NoError(t, job.ArgError())

	// Make sure "wat" is in the known jobs
	assert.EqualValues(t, []string{"wat"}, knownJobs(pool, redisKeyKnownJobs(ns)))

	// Make sure the cache is set
	expiresAt := enqueuer.knownJobs["wat"]
	assert.True(t, expiresAt > (time.Now().Unix()+290))

	// Make sure the length of the queue is 1
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobs(ns, "wat")))

	// Get the job
	j := jobOnQueue(pool, redisKeyJobs(ns, "wat"))
	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())

	// Now enqueue another job, make sure that we can enqueue multiple
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.EqualValues(t, 2, listSize(pool, redisKeyJobs(ns, "wat")))
}

func TestEnqueueIn(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Set to expired value to make sure we update the set of known jobs
	enqueuer.knownJobs["wat"] = 4

	job, err := enqueuer.EnqueueIn("wat", 300, Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	// Make sure "wat" is in the known jobs
	assert.EqualValues(t, []string{"wat"}, knownJobs(pool, redisKeyKnownJobs(ns)))

	// Make sure the cache is set
	expiresAt := enqueuer.knownJobs["wat"]
	assert.True(t, expiresAt > (time.Now().Unix()+290))

	// Make sure the length of the scheduled job queue is 1
	assert.EqualValues(t, 1, zsetSize(pool, redisKeyScheduled(ns)))

	// Get the job
	score, j := jobOnZset(pool, redisKeyScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290)
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
}

func TestEnqueueUnique(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)
	var mutex = &sync.Mutex{}
	job, err := enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
	}

	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Process the queues. Ensure the right number of jobs were processed
	var wats, taws int64
	wp := NewWorkerPool(TestContext{}, 3, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		mutex.Lock()
		wats++
		mutex.Unlock()
		return nil
	})
	wp.JobWithOptions("taw", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		mutex.Lock()
		taws++
		mutex.Unlock()
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	assert.EqualValues(t, 3, wats)
	assert.EqualValues(t, 1, taws)

	// Enqueue again. Ensure we can.
	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Even though taw resulted in an error, we should still be able to re-queue it.
	// This could result in multiple taws enqueued at the same time in a production system.
	job, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

// Tests that unique jobs are removed only after job is done or put in dead queue.
func TestOrderEnqueueUnique(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	enqueuer := NewEnqueuer(ns, pool)
	job, err := enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	require.NotNil(t, job)
	require.NoError(t, err)

	failCount := 0
	doneCh := make(chan struct{})

	// Process the queues.
	backoffCalc := func(job *Job) int64 {
		return 1 // 1 second
	}

	wp := NewWorkerPool(TestContext{}, 3, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 2, Backoff: backoffCalc}, func(job *Job) error {
		failCount++

		switch failCount {
		case 1:
			// We skip this step, because the job goes into retry queue only after this function returns,
			// but we want to test adding a unique job when there is one in retry queue. So we do it at step 2.
		case 2:
			// Try to add the same job, but the original job is in retry queue.
			job, err := enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
			require.Nil(t, job)
			require.NoError(t, err)

			close(doneCh)
		}

		return fmt.Errorf("oops")
	})

	wp.Start()

	select {
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out")
	case <-doneCh:
		wp.Drain()
		wp.Stop()
	}

	// Now the current job is in dead queue, so we can enqueue new one.
	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	require.NotNil(t, job)
	require.NoError(t, err)
}

func TestEnqueueUniqueIn(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Enqueue two unique jobs -- ensure one job sticks.
	job, err := enqueuer.EnqueueUniqueIn("wat", 300, Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	job, err = enqueuer.EnqueueUniqueIn("wat", 10, Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	// Get the job
	score, j := jobOnZset(pool, redisKeyScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290) // We don't want to overwrite the time
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
	assert.True(t, j.Unique)

	// Now try to enqueue more stuff and ensure it
	job, err = enqueuer.EnqueueUniqueIn("wat", 300, Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("taw", 300, nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

func TestEnqueueUniqueByKey(t *testing.T) {
	var arg3 string
	var arg4 string

	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)
	var mutex = &sync.Mutex{}
	job, err := enqueuer.EnqueueUniqueByKey("wat", Q{"a": 3, "b": "foo"}, Q{"key": "123"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "foo", job.ArgString("b"))
		assert.EqualValues(t, 3, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
	}

	job, err = enqueuer.EnqueueUniqueByKey("wat", Q{"a": 3, "b": "bar"}, Q{"key": "123"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUniqueByKey("wat", Q{"a": 4, "b": "baz"}, Q{"key": "124"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueByKey("taw", nil, Q{"key": "125"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Process the queues. Ensure the right number of jobs were processed
	var wats, taws int64
	wp := NewWorkerPool(TestContext{}, 3, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		mutex.Lock()
		argA := job.Args["a"].(float64)
		argB := job.Args["b"].(string)
		if argA == 3 {
			arg3 = argB
		}
		if argA == 4 {
			arg4 = argB
		}

		wats++
		mutex.Unlock()
		return nil
	})
	wp.JobWithOptions("taw", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		mutex.Lock()
		taws++
		mutex.Unlock()
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	assert.EqualValues(t, 2, wats)
	assert.EqualValues(t, 1, taws)

	// Check that arguments got updated to new value
	assert.EqualValues(t, "bar", arg3)
	assert.EqualValues(t, "baz", arg4)

	// Enqueue again. Ensure we can.
	job, err = enqueuer.EnqueueUniqueByKey("wat", Q{"a": 1, "b": "cool"}, Q{"key": "123"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueByKey("wat", Q{"a": 1, "b": "coolio"}, Q{"key": "124"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Even though taw resulted in an error, we should still be able to re-queue it.
	// This could result in multiple taws enqueued at the same time in a production system.
	job, err = enqueuer.EnqueueUniqueByKey("taw", nil, Q{"key": "123"})
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

// Tests that unique by key jobs are removed only after job is done or put in dead queue.
func TestOrderEnqueueUniqueByKey(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	enqueuer := NewEnqueuer(ns, pool)
	job, err := enqueuer.EnqueueUniqueByKey("wat", Q{"a": 1, "b": "cool"}, Q{"key": "123"})
	require.NotNil(t, job)
	require.NoError(t, err)

	failCount := 0
	doneCh := make(chan struct{})

	// Process the queues.
	backoffCalc := func(job *Job) int64 {
		return 1 // 1 second
	}

	wp := NewWorkerPool(TestContext{}, 3, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 2, Backoff: backoffCalc}, func(job *Job) error {
		failCount++

		switch failCount {
		case 1:
			// We skip this step, because the job goes into retry queue only after this function returns,
			// but we want to test adding a unique job when there is one in retry queue. So we do it at step 2.
		case 2:
			// Try to add the same job, but the original job is in retry queue.
			job, err := enqueuer.EnqueueUniqueByKey("wat", Q{"a": 2, "b": "nice"}, Q{"key": "123"})
			require.Nil(t, job)
			require.NoError(t, err)

			close(doneCh)
		}

		return fmt.Errorf("oops")
	})

	wp.Start()

	select {
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out")
	case <-doneCh:
		wp.Drain()
		wp.Stop()
	}

	// Now the current job is in dead queue, so we can enqueue new one.
	job, err = enqueuer.EnqueueUniqueByKey("wat", Q{"a": 3, "b": "awesome"}, Q{"key": "123"})
	require.NotNil(t, job)
	require.NoError(t, err)
}

func TestEnqueueUniqueInByKey(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Enqueue two unique jobs -- ensure one job sticks.
	job, err := enqueuer.EnqueueUniqueInByKey("wat", 300, Q{"a": 1, "b": "cool"}, Q{"key": "123"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	assert.True(t, exists(pool, job.UniqueKey), "unique keys exists")

	job, err = enqueuer.EnqueueUniqueInByKey("wat", 10, Q{"a": 1, "b": "cool"}, Q{"key": "123"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	// Get the job
	score, j := jobOnZset(pool, redisKeyScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290) // We don't want to overwrite the time
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
	assert.True(t, j.Unique)
}

func TestRunEnqueueUniqueInByKey(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Enqueue two unique jobs -- ensure one job sticks.
	job, err := enqueuer.EnqueueUniqueInByKey("wat", 1, Q{"a": 1, "b": "cool"}, Q{"key": "123"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	doneCh := make(chan struct{})
	var argA float64
	var argB string

	wp := NewWorkerPool(TestContext{}, 3, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		argA = job.Args["a"].(float64)
		argB = job.Args["b"].(string)

		close(doneCh)

		return nil
	})

	wp.Start()

	select {
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out")
	case <-doneCh:
		wp.Drain()
		wp.Stop()
	}

	// Make sure the job has run.
	require.EqualValues(t, 1.0, argA)
	require.Equal(t, "cool", argB)

	// Nothing in retries or dead.
	assert.EqualValues(t, 0, zsetSize(pool, redisKeyRetry(ns)), "retry queue must be empty")
	assert.EqualValues(t, 0, zsetSize(pool, redisKeyDead(ns)), "dead queue must be empty")

	// Nothing in the queues or in-progress queues.
	assert.EqualValues(t, 0, listSize(pool, redisKeyScheduled(ns)), "scheduled queue must be empty")
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, "wat")), "jobs queue must be empty")
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, "wat")), "inprocess queue must be empty")

	// No unique keys.
	assert.False(t, exists(pool, job.UniqueKey), "unique keys must be empty")
}

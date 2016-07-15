package work

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})

	assert.Nil(t, err)

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
	err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
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

	err := enqueuer.EnqueueIn("wat", 300, Q{"a": 1, "b": "cool"})

	assert.Nil(t, err)

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

	ok, err := enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Process the queues. Ensure the right numbero of jobs was processed
	var wats, taws int64
	wp := NewWorkerPool(TestContext{}, 3, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		wats++
		return nil
	})
	wp.JobWithOptions("taw", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		taws++
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	assert.EqualValues(t, 3, wats)
	assert.EqualValues(t, 1, taws)

	// Enqueue again. Ensure we can.
	ok, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.True(t, ok)

	// Even though taw resulted in an error, we should still be able to re-queue it.
	// This could result in multiple taws enqueued at the same time in a production system.
	ok, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestEnqueueUniqueIn(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Enqueue two unique jobs -- ensure one job sticks.
	ok, err := enqueuer.EnqueueUniqueIn("wat", 300, Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = enqueuer.EnqueueUniqueIn("wat", 10, Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.False(t, ok)

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
	ok, err = enqueuer.EnqueueUniqueIn("wat", 300, Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = enqueuer.EnqueueUniqueIn("taw", 300, nil)
	assert.NoError(t, err)
	assert.True(t, ok)
}

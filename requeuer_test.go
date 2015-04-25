package work

import (
	// "github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"testing"
	// "fmt"
	// "time"
	// "os"
)

func TestRequeue(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	err = enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	err = enqueuer.EnqueueIn("foo", 10, nil)
	assert.NoError(t, err)
	err = enqueuer.EnqueueIn("foo", 14, nil)
	assert.NoError(t, err)
	err = enqueuer.EnqueueIn("bar", 19, nil)
	assert.NoError(t, err)

	resetNowEpochSecondsMock()

	re := newRequeuer(ns, pool, redisKeyScheduled(ns), []string{"wat", "foo", "bar"})
	re.start()
	re.drain()
	re.stop()

	assert.Equal(t, 2, listSize(pool, redisKeyJobs(ns, "wat")))
	assert.Equal(t, 1, listSize(pool, redisKeyJobs(ns, "foo")))
	assert.Equal(t, 0, listSize(pool, redisKeyJobs(ns, "bar")))
	assert.Equal(t, 2, zsetSize(pool, redisKeyScheduled(ns)))

	j := jobOnQueue(pool, redisKeyJobs(ns, "foo"))
	assert.Equal(t, j.Name, "foo")

	// Because we mocked time to 10 seconds ago above, the job was put on the zset with t=10 secs ago
	// We want to ensure it's requeued with t=now.
	// On boundary conditions with the VM, nowEpochSeconds() might be 1 or 2 secs ahead of EnqueuedAt
	assert.True(t, (j.EnqueuedAt+2) >= nowEpochSeconds())

}

func TestRequeueUnknown(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)

	nowish := nowEpochSeconds()
	setNowEpochSecondsMock(nowish)

	re := newRequeuer(ns, pool, redisKeyScheduled(ns), []string{"bar"})
	re.start()
	re.drain()
	re.stop()

	assert.Equal(t, 0, zsetSize(pool, redisKeyScheduled(ns)))
	assert.Equal(t, 1, zsetSize(pool, redisKeyDead(ns)))

	rank, job := jobOnZset(pool, redisKeyDead(ns))

	assert.Equal(t, nowish, rank)
	assert.Equal(t, nowish, job.FailedAt)
	assert.Equal(t, "unknown job when requeueing", job.LastErr)
}

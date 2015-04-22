package work

import (
	// "fmt"
	// "github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"
)

type TestContext struct{}

func TestApiHeartbeat(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	wp := NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Job("wat", func(job *Job) error { return nil })
	wp.Job("bob", func(job *Job) error { return nil })
	wp.Start()

	wp2 := NewWorkerPool(TestContext{}, 11, ns, pool)
	wp2.Job("foo", func(job *Job) error { return nil })
	wp2.Job("bar", func(job *Job) error { return nil })
	wp2.Start()

	time.Sleep(20 * time.Millisecond)

	client := NewClient(ns, pool)

	ids, err := client.WorkerPoolIDs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ids))
	if len(ids) == 2 {
		expected := []string{wp.workerPoolID, wp2.workerPoolID}
		sort.Strings(expected)
		assert.Equal(t, expected, ids)

		hbs, err := client.WorkerPoolStatuses(ids)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(hbs))

		if len(hbs) == 2 {
			var hbwp, hbwp2 *WorkerPoolStatus

			if wp.workerPoolID == hbs[0].WorkerPoolID {
				hbwp = hbs[0]
				hbwp2 = hbs[1]
			} else {
				hbwp = hbs[1]
				hbwp2 = hbs[0]
			}

			assert.Equal(t, wp.workerPoolID, hbwp.WorkerPoolID)
			assert.Equal(t, uint(10), hbwp.Concurrency)
			assert.Equal(t, []string{"bob", "wat"}, hbwp.JobNames)
			assert.Equal(t, wp.workerIDs(), hbwp.WorkerIDs)

			assert.Equal(t, wp2.workerPoolID, hbwp2.WorkerPoolID)
			assert.Equal(t, uint(11), hbwp2.Concurrency)
			assert.Equal(t, []string{"bar", "foo"}, hbwp2.JobNames)
			assert.Equal(t, wp2.workerIDs(), hbwp2.WorkerIDs)
		}
	}

	wp.Stop()
	wp2.Stop()

	ids, err = client.WorkerPoolIDs()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(ids))
}

func TestApiWorkerStatuses(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue("wat", 1, 2)
	assert.Nil(t, err)
	err = enqueuer.Enqueue("foo", 3, 4)
	assert.Nil(t, err)

	wp := NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Job("wat", func(job *Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	wp.Job("foo", func(job *Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	wp.Start()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(ns, pool)
	statuses, err := client.WorkerStatuses(wp.workerIDs())
	assert.NoError(t, err)

	assert.Equal(t, 10, len(statuses))

	watCount := 0
	fooCount := 0
	for _, status := range statuses {
		if status.JobName == "foo" {
			fooCount++
			assert.True(t, status.IsBusy)
			assert.Equal(t, "[3,4]", status.ArgsJSON)
			assert.True(t, (nowEpochSeconds()-status.StartedAt) <= 3)
			assert.True(t, status.JobID != "")
		} else if status.JobName == "wat" {
			watCount++
			assert.True(t, status.IsBusy)
			assert.Equal(t, "[1,2]", status.ArgsJSON)
			assert.True(t, (nowEpochSeconds()-status.StartedAt) <= 3)
			assert.True(t, status.JobID != "")
		} else {
			assert.False(t, status.IsBusy)
		}
		assert.True(t, status.WorkerID != "")
	}
	assert.Equal(t, 1, watCount)
	assert.Equal(t, 1, fooCount)

	wp.Stop()

	statuses, err = client.WorkerStatuses(wp.workerIDs())
	assert.NoError(t, err)

	assert.Equal(t, 10, len(statuses))
	for _, status := range statuses {
		assert.False(t, status.IsBusy)
		assert.True(t, status.WorkerID != "")
	}
}

func TestApiJobStatuses(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue("wat", 1, 2)
	err = enqueuer.Enqueue("foo", 3, 4)
	err = enqueuer.Enqueue("zaz", 3, 4)

	// Start a pool to work on it. It's going to work on the queues
	// side effect of that is knowing which jobs are avail
	wp := NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Job("wat", func(job *Job) error {
		return nil
	})
	wp.Job("foo", func(job *Job) error {
		return nil
	})
	wp.Job("zaz", func(job *Job) error {
		return nil
	})
	wp.Start()
	time.Sleep(20 * time.Millisecond)
	wp.Stop()

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()
	err = enqueuer.Enqueue("foo", 3, 4)
	setNowEpochSecondsMock(1425263509)
	err = enqueuer.Enqueue("foo", 3, 4)
	setNowEpochSecondsMock(1425263609)
	err = enqueuer.Enqueue("wat", 3, 4)

	setNowEpochSecondsMock(1425263709)
	client := NewClient(ns, pool)
	queues, err := client.Queues()
	assert.NoError(t, err)

	assert.Equal(t, 3, len(queues))
	assert.Equal(t, "foo", queues[0].JobName)
	assert.Equal(t, 2, queues[0].Count)
	assert.Equal(t, 300, queues[0].Latency)
	assert.Equal(t, "wat", queues[1].JobName)
	assert.Equal(t, 1, queues[1].Count)
	assert.Equal(t, 100, queues[1].Latency)
	assert.Equal(t, "zaz", queues[2].JobName)
	assert.Equal(t, 0, queues[2].Count)
	assert.Equal(t, 0, queues[2].Latency)
}

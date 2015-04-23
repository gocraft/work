package work

import (
	"fmt"
	// "github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	// "sort"
	"testing"
	"time"
)

type TestContext struct{}

func TestClientWorkerPoolHeartbeats(t *testing.T) {
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

	hbs, err := client.WorkerPoolHeartbeats()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(hbs))
	if len(hbs) == 2 {
		var hbwp, hbwp2 *WorkerPoolHeartbeat

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

	wp.Stop()
	wp2.Stop()

	hbs, err = client.WorkerPoolHeartbeats()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(hbs))
}

func TestClientWorkerObservations(t *testing.T) {
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
	observations, err := client.WorkerObservations()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(observations))

	watCount := 0
	fooCount := 0
	for _, ob := range observations {
		if ob.JobName == "foo" {
			fooCount++
			assert.True(t, ob.IsBusy)
			assert.Equal(t, "[3,4]", ob.ArgsJSON)
			assert.True(t, (nowEpochSeconds()-ob.StartedAt) <= 3)
			assert.True(t, ob.JobID != "")
		} else if ob.JobName == "wat" {
			watCount++
			assert.True(t, ob.IsBusy)
			assert.Equal(t, "[1,2]", ob.ArgsJSON)
			assert.True(t, (nowEpochSeconds()-ob.StartedAt) <= 3)
			assert.True(t, ob.JobID != "")
		} else {
			assert.False(t, ob.IsBusy)
		}
		assert.True(t, ob.WorkerID != "")
	}
	assert.Equal(t, 1, watCount)
	assert.Equal(t, 1, fooCount)

	// time.Sleep(2000 * time.Millisecond)
	//
	// observations, err = client.WorkerObservations()
	// assert.NoError(t, err)
	// assert.Equal(t, 10, len(observations))
	// for _, ob := range observations {
	// 	assert.False(t, ob.IsBusy)
	// 	assert.True(t, ob.WorkerID != "")
	// }

	wp.Stop()

	observations, err = client.WorkerObservations()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(observations))
}

func TestClientQueues(t *testing.T) {
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

func TestClientScheduledJobs(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	enqueuer := NewEnqueuer(ns, pool)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()
	err := enqueuer.EnqueueIn("wat", 0, 1, 2)
	err = enqueuer.EnqueueIn("zaz", 4, 3, 4)
	err = enqueuer.EnqueueIn("foo", 2, 3, 4)

	client := NewClient(ns, pool)
	jobs, err := client.ScheduledJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(jobs))
	if len(jobs) == 3 {
		assert.Equal(t, 1425263409, jobs[0].RunAt)
		assert.Equal(t, 1425263411, jobs[1].RunAt)
		assert.Equal(t, 1425263413, jobs[2].RunAt)

		assert.Equal(t, "wat", jobs[0].Name)
		assert.Equal(t, "foo", jobs[1].Name)
		assert.Equal(t, "zaz", jobs[2].Name)

		assert.Equal(t, 1425263409, jobs[0].EnqueuedAt)
		assert.Equal(t, 1425263409, jobs[1].EnqueuedAt)
		assert.Equal(t, 1425263409, jobs[2].EnqueuedAt)

		assert.Equal(t, interface{}(1), jobs[0].Args[0])
		assert.Equal(t, interface{}(2), jobs[0].Args[1])

		assert.Equal(t, 0, jobs[0].Fails)
		assert.Equal(t, 0, jobs[1].Fails)
		assert.Equal(t, 0, jobs[2].Fails)

		assert.Equal(t, 0, jobs[0].FailedAt)
		assert.Equal(t, 0, jobs[1].FailedAt)
		assert.Equal(t, 0, jobs[2].FailedAt)

		assert.Equal(t, "", jobs[0].LastErr)
		assert.Equal(t, "", jobs[1].LastErr)
		assert.Equal(t, "", jobs[2].LastErr)
	}
}

func TestClientRetryJobs(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue("wat", 1, 2)
	assert.Nil(t, err)

	setNowEpochSecondsMock(1425263429)

	wp := NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Job("wat", func(job *Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Join()
	wp.Stop()

	client := NewClient(ns, pool)
	jobs, err := client.RetryJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	if len(jobs) == 1 {
		assert.Equal(t, 1425263429, jobs[0].FailedAt)
		assert.Equal(t, "wat", jobs[0].Name)
		assert.Equal(t, 1425263409, jobs[0].EnqueuedAt)
		assert.Equal(t, interface{}(1), jobs[0].Args[0])
		assert.Equal(t, 1, jobs[0].Fails)
		assert.Equal(t, 1425263429, jobs[0].Job.FailedAt)
		assert.Equal(t, "ohno", jobs[0].LastErr)
	}
}

func TestClientDeadJobs(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, pool)
	err := enqueuer.Enqueue("wat", 1, 2)
	assert.Nil(t, err)

	setNowEpochSecondsMock(1425263429)

	wp := NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 0}, func(job *Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Join()
	wp.Stop()

	client := NewClient(ns, pool)
	jobs, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	if len(jobs) == 1 {
		assert.Equal(t, 1425263429, jobs[0].FailedAt)
		assert.Equal(t, "wat", jobs[0].Name)
		assert.Equal(t, 1425263409, jobs[0].EnqueuedAt)
		assert.Equal(t, interface{}(1), jobs[0].Args[0])
		assert.Equal(t, 1, jobs[0].Fails)
		assert.Equal(t, 1425263429, jobs[0].Job.FailedAt)
		assert.Equal(t, "ohno", jobs[0].LastErr)
	}

	// Test pagination a bit
	jobs, err = client.DeadJobs(2)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

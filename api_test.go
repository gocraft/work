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
			assert.Equal(t, 10, hbwp.Concurrency)
			assert.Equal(t, []string{"bob", "wat"}, hbwp.JobNames)

			assert.Equal(t, wp2.workerPoolID, hbwp2.WorkerPoolID)
			assert.Equal(t, 11, hbwp2.Concurrency)
			assert.Equal(t, []string{"bar", "foo"}, hbwp2.JobNames)
		}
	}

	wp.Stop()
	wp2.Stop()

	ids, err = client.WorkerPoolIDs()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(ids))
}

package work

import (
	"testing"
)

func TestWorkerPoolStartStop(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	wp := NewWorkerPool(10, ns, pool)
	wp.Start()
	wp.Start()
	wp.Stop()
	wp.Stop()
	wp.Start()
	wp.Stop()
}

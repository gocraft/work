package health

import (
	"fmt"
	"time"

	"github.com/gocraft/health"
	"github.com/gocraft/work"
)

type QueueReporter struct {
	closed chan struct{}
}

func (r *QueueReporter) Close() error {
	close(r.closed)
	return nil
}

func NewQueueReporter(
	c *work.Client,
	job *health.Job,
	d time.Duration,
) *QueueReporter {
	ch := make(chan struct{})

	go func() {
		for {
			select {
			case <-ch:
				return
			case <-time.After(d):
				job.Run(func() error {
					queues, err := c.Queues()
					if err != nil {
						return err
					}
					for _, queue := range queues {
						job.Gauge(fmt.Sprintf("work_queue.%s.queue_count", queue.JobName), float64(queue.Count))
						job.Timing(fmt.Sprintf("work_queue.%s.latency", queue.JobName), queue.Latency*1e9)
					}
					return nil
				})
			}
		}
	}()

	return &QueueReporter{
		closed: ch,
	}
}

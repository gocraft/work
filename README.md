# gocraft/work [![GoDoc](https://godoc.org/github.com/gocraft/work?status.png)](https://godoc.org/github.com/gocraft/work)

gocraft/work lets you enqueue and processes background jobs in Go. Jobs are durable and backed by Redis. Very similar to Sidekiq for Go.

### Features

* Fast and efficient
* Reliable - don't lose jobs even if your process crashes
* Middleware on jobs -- good for metrics instrumentation, logging, etc.
* If a job fails, it will be retried a specified number of times.
* Schedule jobs to happen in the future
* Web UI to manage failed jobs and observe the system

## Enqueue new jobs

To enqueue jobs, you need to make an Enqueuer with a redis namespace and a redigo pool. Each enqueued job has a name and can take optional arguments. Arguments are k/v pairs (serialized as JSON internally).

```go
package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/gocraft/work"
)

// Make a redis pool
var redisPool = &redis.Pool{
	MaxActive: 5,
	MaxIdle: 5,
	Wait: true,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	},
}

// Make an enqueuer with a particular namespace
var enqueuer = work.NewEnqueuer("my_app_namespace", redisPool)

func main() {
	// Enqueue a job named "send_email" with the specified parameters.
	err := enqueuer.Enqueue("send_email", work.Q{"address": "test@example.com", "subject": "hello world", "customer_id": 4})
    if err != nil {
        log.Fatal(err)
    }
}


```

## Process jobs

In order to process jobs, you'll need to make a WorkerPool. Add middleware and jobs to the pool, and start the pool.

```go
package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/gocraft/work"
	"os"
	"os/signal"
)

// Make a redis pool
var redisPool = &redis.Pool{
	MaxActive: 5,
	MaxIdle: 5,
	Wait: true,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	},
}

type Context struct{
    customerID int64
}

func main() {
	// Make a new pool. Arguments:
	// Context{} is a struct that will be the context for the request.
	// 10 is the max concurrency
	// "my_app_namespace" is the Redis namespace
	// redisPool is a Redis pool
	pool := work.NewWorkerPool(Context{}, 10, "my_app_namespace", redisPool)

	// Add middleware that will be executed for each job
	pool.Middleware((*Context).Log)
	pool.Middleware((*Context).FindCustomer)

	// Map the name of jobs to handler functions
	pool.Job("send_email", (*Context).SendEmail)

	// Customize options:
	pool.JobWithOptions("export", JobOptions{Priority: 10, MaxFails: 1}, (*Context).Export)

	// Start processing jobs
	pool.Start()

	// Wait for a signal to quit:
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan

	// Stop the pool
	pool.Stop()
}

func (c *Context) Log(job *work.Job, next work.NextMiddlewareFunc) error {
	fmt.Println("Starting job: ", job.Name)
	return next()
}

func (c *Context) FindCustomer(job *work.Job, next work.NextMiddlewareFunc) error {
	// If there's a customer_id param, set it in the context for future middleware and handlers to use.
	if _, ok := job.Args["customer_id"]; ok {
		c.customerID = job.ArgInt64("customer_id")
		if err := job.ArgError(); err != nil {
			return err
		}
	}

	return next()
}

func (c *Context) SendEmail(job *work.Job) error {
	// Extract arguments:
	addr := job.ArgString("address")
	subject := job.ArgString("subject")
	if err := job.ArgError(); err != nil {
		return err
	}

	// Go ahead and send the email...
	// sendEmailTo(addr, subject)

	return nil
}

func (c *Context) Export(job *work.Job) error {
	return nil
}
```

## Run the UI

## Design and concepts

### Enqueueing jobs

* When jobs are enqueued, they're serialized with JSON and added to a simple Redis list with LPUSH.
* Jobs are added to a list with the same name as the job. Each job name gets its own queue.

### Scheduling algorithm

* Each job lives in a list-based queue with the same name as the job.
* Each of these queues can have an associated priority. The priority is a number from 1 to 10000.
* Each time a worker pulls a job, it needs to choose a queue. It chooses a queue probabilistically based on its relative priority.
* If the sum of priorities among all queues is 1000, and one queue has priority 100, jobs will be pulled from that queue 10% of the time.
* Obviously if a queue is empty, it won't be considered.
* The semantics of "always process X jobs before Y jobs" can be accurately approximated by giving X a large number (like 10000) and Y a small number (like 1).

### Processing a job

* To process a job, a worker will execute a Lua script to atomically move a job its queue to an in-progress queue.
* The worker will then run the job. The job will either finish successfully or result in an error or panic.
  * If the process completely crashes, the reaper will eventually find it in its in-progress queue and requeue it.
* If the job is successful, we'll simply remove the job from the in-progress queue.
* If the job returns an error or panic, we'll see how many retries a job has left. If it doesn't have any, we'll move it to the dead queue. If it has retries left, we'll consume a retry and add the job to the retry queue. 

### Workers and WorkerPools

* WorkerPools provide the public API of gocraft/work.
  * You can attach jobs and middlware to them.
  * You can start and stop them.
  * Based on their concurrency setting, they'll spin up N worker goroutines.
* Each worker is run in a goroutine. It will get a job from redis, run it, get the next job, etc.
  * Each worker is independent. They are not dispatched work -- they get their own work.

### Retry job, scheduled jobs, and the requeuer

* In addition to the normal list-based queues that normal jobs live in, there are two other types of queues: the retry queue and the scheduled job queue.
* Both of these are implemented as Redis z-sets. The score is the unix timestamp when the job should be run. The value is the bytes of the job.
* The requeuer will occasionally look for jobs in these queues that should be run now. If they should be, they'll be atomically moved to the normal list-based queue and eventually processed.

### Dead jobs

* After a job has failed a specified number of times, it will be added to the dead job queue.
* The dead job queue is just a Redis z-set. The score is the timestamp it failed and the value is the job.
* The dead job queue has a maximum size of X. Old failed jobs will roll off.
* To retry failed jobs, use the UI or the Client API.

### The reaper

* If a process crashes hard (eg, the power on the server turns off or the kernal freezes), some jobs may be in progress and we won't want to lose them. They're safe in their in-progress queue.
* The reaper will look for worker pools without a heartbeat. It will scan their in-progress queues and requeue anything it finds.

### Terminology reference
* "worker pool" - a pool of workers
* "worker" - an individual worker in a single goroutine. Gets a job from redis, does job, gets next job...
* "heartbeater" or "worker pool heartbeater" - goroutine owned by worker pool that runs concurrently with workers. Writes the worker pool's config/status (aka "heartbeat") every 5 seconds.
* "heartbeat" - the status written by the heartbeater.
* "observer" or "worker observer" - observes a worker. Writes stats. makes "observations".
* "worker observation" - A snapshot made by an observer of what a worker is working on.
* "job" - the actual bundle of data that constitutes one job
* "job name" - each job has a name, like "create_watch"
* "job type" - backend/private nomenclature for the handler+options for processing a job
* "queue" - each job creates a queue with the same name as the job. only jobs named X go into the X queue.
* "retry jobs" - If a job fails and needs to be retried, it will be put on this queue.
* "scheduled jobs" - Jobs enqueued to be run in th future will be put on a scheduled job queue.
* "dead jobs" - If a job exceeds its MaxFails count, it will be put on the dead job queue.


## gocraft

gocraft offers a toolkit for building web apps. Currently these packages are available:

* [gocraft/web](https://github.com/gocraft/web) - Go Router + Middleware. Your Contexts.
* [gocraft/dbr](https://github.com/gocraft/dbr) - Additions to Go's database/sql for super fast performance and convenience.
* [gocraft/health](https://github.com/gocraft/health) - Instrument your web apps with logging and metrics.
* [gocraft/work](https://github.com/gocraft/work) - Process background jobs in Go.

These packages were developed by the [engineering team](https://eng.uservoice.com) at [UserVoice](https://www.uservoice.com) and currently power much of its infrastructure and tech stack.

## Authors

* Jonathan Novak -- [https://github.com/cypriss](https://github.com/cypriss)
* Tai-Lin Chu -- [https://github.com/taylorchu](https://github.com/taylorchu)
* Tyler Smith -- [https://github.com/tyler-smith](https://github.com/tyler-smith)
* Sponsored by [UserVoice](https://eng.uservoice.com)

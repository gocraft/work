Jobs
====

A persistent and flexible background jobs library for go.

[![Version](https://img.shields.io/badge/version-0.4.2-5272B4.svg)](https://github.com/albrow/jobs/releases)
[![Circle CI](https://img.shields.io/circleci/project/albrow/jobs.svg)](https://circleci.com/gh/albrow/jobs)
[![GoDoc](https://godoc.org/github.com/albrow/jobs?status.svg)](https://godoc.org/github.com/albrow/jobs)

Jobs is powered by Redis and supports the following features:

 - A job can encapsulate any arbitrary functionality. A job can do anything
   which can be done in a go function.
 - A job can be one-off (only executed once) or recurring (scheduled to
   execute at a specific interval).
 - A job can be retried a specified number of times if it fails.
 - A job is persistent, with protections against power loss and other worst
   case scenarios. (See the [Guarantees](#guarantees) section below)
 - Work on jobs can be spread amongst any number of concurrent workers across any
   number of machines.
 - Provided it is persisted to disk, every job will be executed *at least* once,
 	and in ideal network conditions will be executed *exactly* once. (See the
 	[Guarantees](#guarantees) section below)
 - You can query the database to find out e.g. the number of jobs that are
   currently executing or how long a particular job took to execute.
 - Any job that permanently fails will have its error captured and stored.


Why is it Useful?
-----------------

Jobs is intended to be used in web applications. It is useful for cases where you need
to execute some long-running code, but you don't want your users to wait for the code to
execute before rendering a response. A good example is sending a welcome email to your users
after they sign up. You can use Jobs to schedule the email to be sent asynchronously, and
render a response to your user without waiting for the email to be sent. You could use a
goroutine to accomplish the same thing, but in the event of a server restart or power loss,
the email might never be sent. Jobs guarantees that the email will be sent at some time
and allows you to spread the work between different machines.


Development Status
------------------

Jobs is being actively developed and is well-tested. However, since it is still a young library,
it is not recommended for use in mission-critical production applications at this time. It is probably
fine to use for low-traffic hobby sites, and in fact we encourage its use in those settings to help
polish the API and find missing features and hidden bugs. Pull requests and issue reports are much
appreciated :)

Jobs follows semantic versioning but offers no guarantees of backwards compatibility until version
1.0. Keep in mind that breaking changes might occur. We will do our best to make the community aware
of any non-trivial breaking changes beforehand.

Installation
------------

Jobs requires Go version >= 1.2. If you do not already have it, follow these instructions:

- [Install Go](http://golang.org/doc/install)
- Follow the instructions for [setting up your go workspace](https://golang.org/doc/code.html)

Jobs requires access to a Redis database. If you plan to have multiple worker pools spread
out across different machines, they should all connect to the same Redis database. If you
only want to run one worker pool, it is safe to install Redis locally and run it on the same
machine. In either case, if you need to install Redis, follow these instructions:

- [Install Redis](http://redis.io/download).
- Follow the instructions in the section called
  [Installing Redis more properly](http://redis.io/topics/quickstart#installing-redis-more-properly).
- Make sure you understand how [Redis Persistence](http://redis.io/topics/persistence) works and have
  edited your config file to get your desired persistence. We recommend using both RDB and AOF and setting
  fsync to either "always" or "everysec".

After that, you can install Jobs like you would any other go package: `go get github.com/albrow/jobs`.
If you want to update the package later, use `go get -u github.com/albrow/jobs`. Then you can import
Jobs like you would any other go package by adding `import github.com/albrow/jobs` to your go source
file.


Quickstart Guide
----------------

### Connecting to Redis

You can configure the connection to Redis by editing Config.Db. Here are the options:

- Address is the address of the redis database to connect to. Default is
  "localhost:6379".
- Network is the type of network to use to connect to the redis database
Default is "tcp".
- Database is the redis database number to use for storing all data. Default
  is 0.
- Password is a password to use for connecting to a redis database via the
  AUTH command. If empty, Jobs will not attempt to authenticate. Default is
  "" (an empty string).

You should edit Config.Db during program initialization, before running Pool.Start
or scheduling any jobs. Here's an example of how to configure Jobs to use databse #10
and authenticate with the password "foobar":

``` go
func main() {
	// Configure database options at the start of your application
	jobs.Config.Db.Database = 10
	jobs.Config.Db.Password = "foobar"
}
```

### Registering Job Types

Jobs must be organized into discrete types. Here's an example of how to register a job
which sends a welcome email to users:

``` go
// We'll specify that we want the job to be retried 3 times before finally failing
welcomeEmailJobs, err := jobs.RegisterType("welcomeEmail", 3, func(user *User) error {
	msg := fmt.Sprintf("Hello, %s! Thanks for signing up for foo.com.", user.Name)
	if err := emails.Send(user.EmailAddress, msg); err != nil {
		// The returned error will be captured by a worker, which will then log the error
		// in the database and trigger up to 3 retries.
		return err
	}
})
```

The final argument to the RegisterType function is a [HandlerFunc](http://godoc.org/github.com/albrow/jobs#HandlerFunc)
which will be executed when the job runs. HandlerFunc must be a function which accepts either
zero or one arguments and returns an error.

### Scheduling a Job

After registering a job type, you can schedule a job using the Schedule or ScheduleRecurring
methods like so:

``` go
// The priority argument lets you choose how important the job is. Higher
// priority jobs will be executed first.
job, err := welcomeEmailJobs.Schedule(100, time.Now(), &User{EmailAddress: "foo@example.com"})
if err != nil {
	// Handle err
}
```

You can use the [Job object](http://godoc.org/github.com/albrow/jobs#Job) returned by Schedule
or ScheduleRecurring to check on the status of the job or cancel it manually.

### Starting and Configuring Worker Pools

You can schedule any number of worker pools across any number of machines, provided every machine
agrees on the definition of the job types. If you want, you can start a worker pool on the same
machines that are scheduling jobs, or you can have each worker pool running on a designated machine.
Since each pool is assigned an id based on a unique hardware identifier, you must only run one
worker pool per machine.

To create a new pool with the [default configuration](http://godoc.org/github.com/albrow/jobs#pkg-variables),
just pass in nil:

``` go
pool, err := jobs.NewPool(nil)
if err != nil {
	// Handle err
}
```

You can also specify a different configuration by passing in
[*PoolConfig](http://godoc.org/github.com/albrow/jobs#PoolConfig). Any zero values in the config you pass
in will fallback to the default values. So here's how you could start a pool with 10 workers and a batch
size of 10, while letting the other options remain the default.

``` go
pool, err := jobs.NewPool(&jobs.PoolConfig{
	NumWorkers: 10,
	BatchSize: 10,
})
if err != nil {
	// Handle err
}
```

After you have created a pool, you can start it with the Start method. Once started, the pool will
continuously query the database for new jobs and delegate those jobs to workers. Any program that calls
Pool.Start() should also wait for the workers to finish before exiting. You can do so by wrapping Close and
Wait in a defer statement. Typical usage looks something like this:

``` go
func main() {
	pool, err := jobs.NewPool(nil)
	if err != nil {
		// Handle err
	}
	defer func() {
		pool.Close()
		if err := pool.Wait(); err != nil {
			// Handle err
		}
	}()
	if err := pool.Start(); err != nil {
		// Handle err
	}
}
```

You can also call Close and Wait at any time to manually stop the pool from executing new jobs. In this
case, any jobs that are currently being executed will still finish.


Testing
-------

To run the tests, make sure you have Redis running and accepting unix socket connections on the address
/tmp/redis.sock. The tests will use database #14. **WARNING:** After each test is run, database #14 will be completely
erased, so make sure you do not have any important data stored there.

To run the tests just run `go test .` If anything fails, please report an issue and describe what happened.


Contributing
------------

See [Contributing.md](https://github.com/albrow/jobs/blob/master/CONTRIBUTING.md)


Guarantees
-----------

### Persistence

Since jobs is powered by Redis, there is a chance that you can lose data with the default Redis configuration.
To get the best persistence guarantees, you should set Redis to use both AOF and RDB persistence modes and set
fsync to "always". With these settings, Redis is more or less
[as persistent as a database like postgres](http://redis.io/topics/persistence#ok-so-what-should-i-use). If want
better performance and are okay with a slightly greater chance of losing data (i.e. jobs not executing), you can
set fsync to "everysec".

[Read more about Redis persistence](http://redis.io/topics/persistence).

### Atomicity

Jobs is carefully written using Redis transactions and lua scripting so that all database changes are atomic.
If Redis crashes in the middle of a transaction or script execution, it is possible that your AOF file can become
corrupted. If this happens, Redis will refuse to start until the AOF file is fixed. It is relatively easy to fix
the problem with the redis-check-aof tool, which will remove the partial transaction from the AOF file. In effect,
this guarantees that modifications of the database are atomic, even in the event of a power loss or hard reset,
with the caveat that you may need to use the redis-check-aof tool in the worst case scenario.

Read more about [Redis transactions](http://redis.io/topics/transactions) and
[scripts](http://redis.io/commands#scripting).

### Job Execution

Jobs guarantees that a job will be executed *at least* once, provided it has been persisted on disk. (See the section
on [Persistence](#persistence) directly above). A job can only picked up by one pool at a time because a pool
atomically pops (gets and immediately moves) the next available jobs from the database. A job can only be executed
by one worker at a time because the jobs are delegated to workers via a shared channel. Each worker pool checks on
the health of all the other pools when it starts. If a pool crashes or is otherwise disconnected, any jobs it had
grabbed from the database that did not yet finish will be re-queued and picked up by a different pool.

This is in no way an exhaustive list, but here are some known examples of scenarios that may cause a job to be
executed more than once:

1. If there is a power failure or hard reset while a worker is in the middle of executing a job, the job may be
   stuck in a half-executed state. Since there is no way to know how much of the job was successfully completed,
   the job will be re-queued and picked up by a different pool, where it may be partially or fully executed
   more than once.
2. If a pool becomes disconnected, it will be considered stale and its jobs will be re-queued and reclaimed
   by a different pool. However, if the stale pool is able to partly or fully execute jobs without a reliable
   internet connection, any jobs belonging to the stale pool might be executed more than once. You can increase
   the [StaleTimeout](https://godoc.org/github.com/albrow/jobs#PoolConfig) parameter for a pool to make this
   scenario less likely.


License
-------

Jobs is licensed under the MIT License. See the LICENSE file for more information.

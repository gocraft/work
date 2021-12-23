// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Pool is a pool of workers. Pool will query the database for queued jobs
// and delegate those jobs to some number of workers. It will do this continuously
// until the main program exits or you call Pool.Close().
type Pool struct {
	// config holds all config options for the pool
	config *PoolConfig
	// id is a unique identifier for each worker, which is generated whenver
	// Start() is called
	id string
	// workers is a slice of all workers
	workers []*worker
	// jobs is a channel through which jobs are delegated to workers
	jobs chan *Job
	// wg can be used after the jobs channel is closed to wait for all
	// workers to finish executing their current jobs.
	wg *sync.WaitGroup
	// exit is used to signal the pool to stop running the query loop
	// and close the jobs channel
	exit chan bool
	// afterFunc is a function that gets called after each job.
	afterFunc func(*Job)
	// RWMutex is only used during testing when we need to
	// change some of the fields for the pool after it was started.
	// NOTE: currently only used in one test (TestStalePoolsArePurged)
	// and might be removed if we refactor later.
	sync.RWMutex
}

// PoolConfig is a set of configuration options for pools. Setting any value
// to the zero value will be interpretted as the default.
type PoolConfig struct {
	// NumWorkers is the number of workers to run
	// Each worker will run inside its own goroutine
	// and execute jobs asynchronously. Default is
	// runtime.GOMAXPROCS.
	NumWorkers int
	// BatchSize is the number of jobs to send through
	// the jobs channel at once. Increasing BatchSize means
	// the worker pool will query the database less frequently,
	// so you would get higher performance. However this comes
	// at the cost that jobs with lower priority may sometimes be
	// executed before jobs with higher priority, because the jobs
	// with higher priority were not ready yet the last time the pool
	// queried the database. Decreasing BatchSize means more
	// frequent queries to the database and lower performance, but
	// greater likelihood of executing jobs in perfect order with regards
	// to priority. Setting BatchSize to 1 gaurantees that higher priority
	// jobs are always executed first as soon as they are ready. Default is
	// runtime.GOMAXPROCS.
	BatchSize int
	// MinWait is the minimum amount of time the pool will wait before checking
	// the database for queued jobs. The pool may take longer to query the database
	// if the jobs channel is blocking (i.e. if no workers are ready to execute new
	// jobs). Default is 200ms.
	MinWait time.Duration
	// StaleTimeout is the amount of time to wait for a pool to reply to a ping request
	// before considering it stale. Stale pools will be purged and if they have any
	// corresponding jobs in the executing set, those jobs will be requeued. Default
	// is 30 seconds.
	StaleTimeout time.Duration
}

// DefaultPoolConfig is the default config for pools. You can override any values
// by passing in a *PoolConfig to NewPool. Any zero values in PoolConfig will be
// interpreted as the default.
var DefaultPoolConfig = &PoolConfig{
	NumWorkers:   runtime.GOMAXPROCS(0),
	BatchSize:    runtime.GOMAXPROCS(0),
	MinWait:      200 * time.Millisecond,
	StaleTimeout: 30 * time.Second,
}

// NewPool creates and returns a new pool with the given configuration. You can
// pass in nil to use the default values. Otherwise, any zero values in config will
// be interpreted as the default value.
func NewPool(config *PoolConfig) (*Pool, error) {
	finalConfig := getPoolConfig(config)
	hardwareId, err := getHardwareId()
	if err != nil {
		return nil, err
	}
	return &Pool{
		config:  finalConfig,
		id:      hardwareId,
		wg:      &sync.WaitGroup{},
		exit:    make(chan bool),
		workers: make([]*worker, finalConfig.NumWorkers),
		jobs:    make(chan *Job, finalConfig.BatchSize),
	}, nil
}

// getPoolConfig replaces any zero values in passedConfig with the default values.
// If passedConfig is nil, every value will be set to the default.
func getPoolConfig(passedConfig *PoolConfig) *PoolConfig {
	if passedConfig == nil {
		return DefaultPoolConfig
	}
	finalConfig := &PoolConfig{}
	(*finalConfig) = (*passedConfig)
	if passedConfig.NumWorkers == 0 {
		finalConfig.NumWorkers = DefaultPoolConfig.NumWorkers
	}
	if passedConfig.BatchSize == 0 {
		finalConfig.BatchSize = DefaultPoolConfig.BatchSize
	}
	if passedConfig.MinWait == 0 {
		finalConfig.MinWait = DefaultPoolConfig.MinWait
	}
	if passedConfig.StaleTimeout == 0 {
		finalConfig.StaleTimeout = DefaultPoolConfig.StaleTimeout
	}
	return finalConfig
}

// addToPoolSet adds the id of the worker pool to a set of active pools
// in the database.
func (p *Pool) addToPoolSet() error {
	conn := redisPool.Get()
	defer conn.Close()
	p.RLock()
	thisId := p.id
	p.RUnlock()
	if _, err := conn.Do("SADD", Keys.ActivePools, thisId); err != nil {
		return err
	}
	return nil
}

// removeFromPoolSet removes the id of the worker pool from a set of active pools
// in the database.
func (p *Pool) removeFromPoolSet() error {
	conn := redisPool.Get()
	defer conn.Close()
	p.RLock()
	thisId := p.id
	p.RUnlock()
	if _, err := conn.Do("SREM", Keys.ActivePools, thisId); err != nil {
		return err
	}
	return nil
}

// getHardwareId returns a unique identifier for the current machine. It does this
// by iterating through the network interfaces of the machine and picking the first
// one that has a non-empty hardware (MAC) address. MAC Addresses are guaranteed by
// IEEE to be unique, however, they are also sometimes spoofable. Spoofed MAC addresses
// are fine as long as no two machines in the job pool have the same MAC address.
func getHardwareId() (string, error) {
	inters, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("jobs: Unable to get network interfaces via net.Interfaces(). Does this machine have any network interfaces?\n%s", err.Error())
	}
	address := ""
	for _, inter := range inters {
		if inter.HardwareAddr.String() != "" {
			address = inter.HardwareAddr.String()
			break
		}
	}
	if address == "" {
		return "", fmt.Errorf("jobs: Unable to find a network interface with a non-empty hardware (MAC) address. Does this machine have any valid network interfaces?\n%s", err.Error())
	}
	return address, nil
}

// pingKey is the key for a pub/sub connection which allows a pool to ping, i.e.
// check the status of, another pool.
func (p *Pool) pingKey() string {
	p.RLock()
	thisId := p.id
	p.RUnlock()
	return "workers:" + thisId + ":ping"
}

// pongKey is the key for a pub/sub connection which allows a pool to respond to
// pings with a pong, i.e. acknowledge that it is still alive and working.
func (p *Pool) pongKey() string {
	p.RLock()
	thisId := p.id
	p.RUnlock()
	return "workers:" + thisId + ":pong"
}

// purgeStalePools will first get all the ids of pools from the activePools
// set. All of these should be active, but if Pool.Wait was never called for
// a pool (possibly because of power failure), some of them might not actually
// be active. To find out for sure, purgeStalePools will ping each pool that is
// supposed to be active and wait for a pong response. If it does not receive
// a pong within some amount of time, the pool is considered stale (i.e. whatever
// process that was running it was exited and it is no longer executing jobs). If
// any stale pools are found, purgeStalePools will remove them from the set of
// active pools and then moves any jobs associated with the stale pool from the
// executing set to the queued set to be retried.
func (p *Pool) purgeStalePools() error {
	conn := redisPool.Get()
	defer conn.Close()
	poolIds, err := redis.Strings(conn.Do("SMEMBERS", Keys.ActivePools))
	if err != nil {
		return err
	}
	for _, poolId := range poolIds {
		p.RLock()
		thisId := p.id
		p.RUnlock()
		if poolId == thisId {
			// Don't ping self
			continue
		}
		pool := &Pool{id: poolId}
		go func(pool *Pool) {
			if err := p.pingAndPurgeIfNeeded(pool); err != nil {
				// TODO: send accross an err channel instead of panicking
				panic(err)
			}
		}(pool)
	}
	return nil
}

// pingAndPurgeIfNeeded pings other by publishing to others ping key. If it
// does not receive a pong reply within some amount of time, it will
// assume the pool is stale and purge it.
func (p *Pool) pingAndPurgeIfNeeded(other *Pool) error {
	ping := redisPool.Get()
	pong := redis.PubSubConn{redisPool.Get()}
	// Listen for pongs by subscribing to the other pool's pong key
	pong.Subscribe(other.pongKey())
	// Ping the other pool by publishing to its ping key
	ping.Do("PUBLISH", other.pingKey(), 1)
	// Use a select statement to either receive the pong or timeout
	pongChan := make(chan interface{})
	errChan := make(chan error)
	go func() {
		defer func() {
			pong.Close()
			ping.Close()
		}()
		select {
		case <-p.exit:
			return
		default:
		}
		for {
			reply := pong.Receive()
			switch reply.(type) {
			case redis.Message:
				// The pong was received
				pongChan <- reply
				return
			case error:
				// There was some unexpected error
				err := reply.(error)
				errChan <- err
				return
			}
		}
	}()
	timeout := time.After(p.config.StaleTimeout)
	select {
	case <-pongChan:
		// The other pool responded with a pong
		return nil
	case err := <-errChan:
		// Received an error from the pubsub conn
		return err
	case <-timeout:
		// The pool is considered stale and should be purged
		t := newTransaction()
		other.RLock()
		otherId := other.id
		other.RUnlock()
		t.purgeStalePool(otherId)
		if err := t.exec(); err != nil {
			return err
		}
	}
	return nil
}

// respondToPings continuously listens for pings from other worker pools and
// immediately responds with a pong. It will only return if there is an error.
func (p *Pool) respondToPings() error {
	pong := redisPool.Get()
	ping := redis.PubSubConn{redisPool.Get()}
	defer func() {
		pong.Close()
		ping.Close()
	}()
	// Subscribe to the ping key for this pool to receive pings.
	if err := ping.Subscribe(p.pingKey()); err != nil {
		return err
	}
	for {
		// Whenever we recieve a ping, reply immediately with a pong by
		// publishing to the pong key for this pool.
		switch reply := ping.Receive().(type) {
		case redis.Message:
			if _, err := pong.Do("PUBLISH", p.pongKey(), 0); err != nil {
				return err
			}
		case error:
			err := reply.(error)
			return err
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// removeStaleSelf will check if the current machine recently failed hard
// (e.g. due to power failuer) by checking if p.id is in the set of active
// pools. If p.id is still "active" according to the database, it means
// there was a hard failure, and so removeStaleSelf then re-queues the
// stale jobs. removeStaleSelf should only be run when the Pool is started.
func (p *Pool) removeStaleSelf() error {
	t := newTransaction()
	t.purgeStalePool(p.id)
	if err := t.exec(); err != nil {
		return err
	}
	return nil
}

// SetAfterFunc will assign a function that will be executed each time
// a job is finished.
func (p *Pool) SetAfterFunc(f func(*Job)) {
	p.afterFunc = f
}

// Start starts the worker pool. This means the pool will initialize workers,
// continuously query the database for queued jobs, and delegate those jobs
// to the workers.
func (p *Pool) Start() error {
	// Purge stale jobs belonging to this pool if there was a recent
	// hard failure
	if err := p.removeStaleSelf(); err != nil {
		return err
	}

	// Check on the status of other worker pools by pinging them and
	// start the process to repond to pings from other pools
	if err := p.addToPoolSet(); err != nil {
		return err
	}
	go func() {
		select {
		case <-p.exit:
			return
		default:
		}
		if err := p.respondToPings(); err != nil {
			// TODO: send the err accross a channel instead of panicking
			panic(err)
		}
	}()
	if err := p.purgeStalePools(); err != nil {
		return err
	}

	// Initialize workers
	for i := range p.workers {
		p.wg.Add(1)
		worker := &worker{
			wg:        p.wg,
			jobs:      p.jobs,
			afterFunc: p.afterFunc,
		}
		p.workers[i] = worker
		worker.start()
	}
	go func() {
		if err := p.queryLoop(); err != nil {
			// TODO: send the err accross a channel instead of panicking
			panic(err)
		}
	}()
	return nil
}

// Close closes the worker pool and prevents it from delegating
// any new jobs. However, any jobs that are currently being executed
// will still be executed. Close returns immediately. If you want to
// wait until all workers are done executing their current jobs, use the
// Wait method.
func (p *Pool) Close() {
	close(p.exit)
}

// Wait will return when all workers are done executing their jobs.
// Wait can only possibly return after you have called Close. To prevent
// errors due to partially-executed jobs, any go program which starts a
// worker pool should call Wait (and Close before that if needed) before
// exiting.
func (p *Pool) Wait() error {
	// The shared waitgroup will only return after each worker is finished
	p.wg.Wait()
	// Remove the pool id from the set of active pools, only after we know
	// each worker finished executing.
	if err := p.removeFromPoolSet(); err != nil {
		return err
	}
	return nil
}

// queryLoop continuously queries the database for new jobs and, if
// it finds any, sends them through the jobs channel for execution
// by some worker.
func (p *Pool) queryLoop() error {
	if err := p.sendNextJobs(p.config.BatchSize); err != nil {
		return err
	}
	for {
		minWait := time.After(p.config.MinWait)
		select {
		case <-p.exit:
			// Close the channel to tell workers to stop executing new jobs
			close(p.jobs)
			return nil
		case <-minWait:
			if err := p.sendNextJobs(p.config.BatchSize); err != nil {
				return err
			}
		}
	}
	return nil
}

// sendNextJobs queries the database to find the next n ready jobs, then
// sends those jobs to the jobs channel, effectively delegating them to
// a worker.
func (p *Pool) sendNextJobs(n int) error {
	jobs, err := p.getNextJobs(p.config.BatchSize)
	if err != nil {
		return err
	}
	// Send the jobs across the channel, where they will be picked up
	// by exactly one worker
	for _, job := range jobs {
		p.jobs <- job
	}
	return nil
}

// getNextJobs queries the database and returns the next n ready jobs.
func (p *Pool) getNextJobs(n int) ([]*Job, error) {
	p.RLock()
	thisId := p.id
	p.RUnlock()
	return getNextJobs(n, thisId)
}

// getNextJobs queries the database and returns the next n ready jobs.
func getNextJobs(n int, poolId string) ([]*Job, error) {
	// Start a new transaction
	t := newTransaction()
	// Invoke a script to get all the jobs which are ready to execute based on their
	// time parameter and whether or not they are in the queued set.
	jobs := []*Job{}
	t.popNextJobs(n, poolId, newScanJobsHandler(&jobs))

	// Execute the transaction
	if err := t.exec(); err != nil {
		return nil, err
	}
	return jobs, nil
}

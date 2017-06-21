package work

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func redisNamespacePrefix(namespace string) string {
	l := len(namespace)
	if (l > 0) && (namespace[l-1] != ':') {
		namespace = namespace + ":"
	}
	return namespace
}

func redisKeyKnownJobs(namespace string) string {
	return redisNamespacePrefix(namespace) + "known_jobs"
}

// returns "<namespace>:jobs:"
// so that we can just append the job name and be good to go
func redisKeyJobsPrefix(namespace string) string {
	return redisNamespacePrefix(namespace) + "jobs:"
}

func redisKeyJobs(namespace, jobName string) string {
	return redisKeyJobsPrefix(namespace) + jobName
}

func redisKeyJobsInProgress(namespace, poolID, jobName string) string {
	return fmt.Sprintf("%s:%s:inprogress", redisKeyJobs(namespace, jobName), poolID)
}

func redisKeyRetry(namespace string) string {
	return redisNamespacePrefix(namespace) + "retry"
}

func redisKeyDead(namespace string) string {
	return redisNamespacePrefix(namespace) + "dead"
}

func redisKeyScheduled(namespace string) string {
	return redisNamespacePrefix(namespace) + "scheduled"
}

func redisKeyWorkerObservation(namespace, workerID string) string {
	return redisNamespacePrefix(namespace) + "worker:" + workerID
}

func redisKeyWorkerPools(namespace string) string {
	return redisNamespacePrefix(namespace) + "worker_pools"
}

func redisKeyHeartbeat(namespace, workerPoolID string) string {
	return redisNamespacePrefix(namespace) + "worker_pools:" + workerPoolID
}

var pauseKeySuffix = "paused"
func redisKeyJobsPaused(namespace, jobName string) string {
	return redisKeyJobs(namespace, jobName) + ":" + pauseKeySuffix
}

var lockKeySuffix = "lock"
func redisKeyJobsLock(namespace, jobName string) string {
	return redisKeyJobs(namespace, jobName) + ":" + lockKeySuffix
}

var lockInfoKeySuffix = "lock_info"
func redisKeyJobsLockInfo(namespace, jobName string) string {
	return redisKeyJobs(namespace, jobName) + ":" + lockInfoKeySuffix
}

var concurrencyKeySuffix = "max_concurrency"
func redisKeyJobsConcurrency(namespace, jobName string) string {
	return redisKeyJobs(namespace, jobName) + ":" + concurrencyKeySuffix
}

func redisKeyUniqueJob(namespace, jobName string, args map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	buf.WriteString(redisNamespacePrefix(namespace))
	buf.WriteString("unique:")
	buf.WriteString(jobName)
	buf.WriteRune(':')

	if args != nil {
		err := json.NewEncoder(&buf).Encode(args)
		if err != nil {
			return "", err
		}
	}

	return buf.String(), nil
}

func redisKeyLastPeriodicEnqueue(namespace string) string {
	return redisNamespacePrefix(namespace) + "last_periodic_enqueue"
}

// Helpers functions used by Lua scripts below that need to match naming convention as redisKeyJobs* functions above
// note: all assume the local var jobQueue is in scope, which is the the val of redisKeyJobs()
// note: acquire/release lock functions assume getLockKey and getLockKeyInfo are in scope
var redisLuaJobsPausedKey = fmt.Sprintf(`
local function getPauseKey(jobQueue)
  return string.format("%%s:%s", jobQueue)
end`, pauseKeySuffix)

var redisLuaJobsLockKey = fmt.Sprintf(`
local function getLockKey(jobQueue)
  return string.format("%%s:%s", jobQueue)
end`, lockKeySuffix)

var redisLuaJobsLockInfoKey = fmt.Sprintf(`
local function getLockInfoKey(jobQueue)
  return string.format("%%s:%s", jobQueue)
end`, lockInfoKeySuffix)

var redisLuaJobsConcurrencyKey = fmt.Sprintf(`
local function getConcurrencyKey(jobQueue)
  return string.format("%%s:%s", jobQueue)
end`, concurrencyKeySuffix)

var redisLuaAcquireLock = fmt.Sprintf(`
local function acquireLock(jobQueue, workerPoolID)
  redis.call('incr', getLockKey(jobQueue))
  redis.call('hincrby', getLockInfoKey(jobQueue), workerPoolID, 1)
end
`)

var redisLuaReleaseLock = fmt.Sprintf(`
local function releaseLock(jobQueue, workerPoolID)
  redis.call('decr', getLockKey(jobQueue))
  redis.call('hincrby', getLockInfoKey(jobQueue), workerPoolID, -1)
end
`)

// Used to fetch the next job to run
//
// KEYS[1] = the 1st job queue we want to try, eg, "work:jobs:emails"
// KEYS[2] = the 1st job queue's in prog queue, eg, "work:jobs:emails:97c84119d13cb54119a38743:inprogress"
// KEYS[3] = the 2nd job queue...
// KEYS[4] = the 2nd job queue's in prog queue...
// ...
// KEYS[N] = the last job queue...
// KEYS[N+1] = the last job queue's in prog queue...
// ARGV[1] = job queue's workerPoolID
var redisLuaFetchJob = fmt.Sprintf(`
-- getPauseKey will be inserted below
%s
-- getLockKey will be inserted below
%s
-- getLockInfoKey will be inserted below
%s
-- getConcurrencyKey will be inserted below
%s
-- acquireLock will be inserted below
%s

local function haveJobs(jobQueue)
  return redis.call('llen', jobQueue) > 0
end

local function isPaused(pauseKey)
  return redis.call('get', pauseKey)
end

local function canRun(lockKey, maxConcurrency)
  local activeJobs = tonumber(redis.call('get', lockKey))
  if (not maxConcurrency or maxConcurrency == 0) or (not activeJobs or activeJobs < maxConcurrency) then
    -- default case: maxConcurrency not defined or set to 0 means no cap on concurrent jobs OR
    -- maxConcurrency set, but lock does not yet exist OR
    -- maxConcurrency set, lock is set, but not yet at max concurrency
    return true
  else
    -- we are at max capacity for running jobs
    return false
  end
end

local res, jobQueue, inProgQueue, pauseKey, lockKey, maxConcurrency, workerPoolID
local keylen = #KEYS
workerPoolID = ARGV[1]

for i=1,keylen,2 do
  jobQueue = KEYS[i]
  inProgQueue = KEYS[i+1]
  pauseKey = getPauseKey(jobQueue)
  lockKey = getLockKey(jobQueue)
  maxConcurrency = tonumber(redis.call('get', getConcurrencyKey(jobQueue)))

  if haveJobs(jobQueue) and not isPaused(pauseKey) and canRun(lockKey, maxConcurrency) then
    res = redis.call('rpoplpush', jobQueue, inProgQueue)
    if res then
      acquireLock(jobQueue, workerPoolID)
      return {res, jobQueue, inProgQueue}
    end
  end
end
return nil`, redisLuaJobsPausedKey, redisLuaJobsLockKey, redisLuaJobsLockInfoKey, redisLuaJobsConcurrencyKey, redisLuaAcquireLock)

// Used by the reaper to re-enqueue jobs that were in progress
//
// KEYS[1] = the 1st job's in progress queue
// KEYS[2] = the 1st job's job queue
// KEYS[3] = the 2nd job's in progress queue
// KEYS[4] = the 2nd job's job queue
// ...
// KEYS[N] = the last job's in progress queue
// KEYS[N+1] = the last job's job queue
// ARGV[1] = workerPoolID for job queue
var redisLuaReenqueueJob = fmt.Sprintf(`
-- getLockKey inserted below
%s
-- getLockInfoKey will be inserted below
%s
-- releaseLock will be inserted below
%s

local keylen = #KEYS
local res, jobQueue, inProgQueue, workerPoolID
workerPoolID = ARGV[1]
for i=1,keylen,2 do
  inProgQueue = KEYS[i]
  jobQueue = KEYS[i+1]
  res = redis.call('rpoplpush', inProgQueue, jobQueue)
  if res then
    releaseLock(jobQueue, workerPoolID)
    return {res, inProgQueue, jobQueue}
  end
end
return nil`, redisLuaJobsLockKey, redisLuaJobsLockInfoKey, redisLuaReleaseLock)

// Used by the reaper to clean up stale locks
//
// KEYS[1] = the 1st job's lock
// KEYS[2] = the 1st job's lock info hash
// KEYS[3] = the 2nd job's lock
// KEYS[4] = the 2nd job's lock info hash
// ...
// KEYS[N] = the last job's lock
// KEYS[N+1] = the last job's lock info haash
// ARGV[1] = the dead worker pool id
var redisLuaReapStaleLocks =  `
local keylen = #KEYS
local lock, lockInfo, deadLockCount
local deadPoolID = ARGV[1]

for i=1,keylen,2 do
  lock = KEYS[i]
  lockInfo = KEYS[i+1]
  deadLockCount = tonumber(redis.call('hget', lockInfo, deadPoolID))

  if deadLockCount then
    redis.call('decrby', lock, deadLockCount)
    redis.call('hdel', lockInfo, deadPoolID)

    if tonumber(redis.call('get', lock)) < 0 then
      redis.call('set', lock, 0)
    end
  end
end
return nil
`

// KEYS[1] = zset of jobs (retry or scheduled), eg work:retry
// KEYS[2] = zset of dead, eg work:dead. If we don't know the jobName of a job, we'll put it in dead.
// KEYS[3...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
var redisLuaZremLpushCmd = `
local res, j, queue
res = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, 1)
if #res > 0 then
  j = cjson.decode(res[1])
  redis.call('zrem', KEYS[1], res[1])
  queue = ARGV[1] .. j['name']
  for _,v in pairs(KEYS) do
    if v == queue then
      j['t'] = tonumber(ARGV[2])
      redis.call('lpush', queue, cjson.encode(j))
      return 'ok'
    end
  end
  j['err'] = 'unknown job when requeueing'
  j['failed_at'] = tonumber(ARGV[2])
  redis.call('zadd', KEYS[2], ARGV[2], cjson.encode(j))
  return 'dead' -- put on dead queue
end
return nil
`

// KEYS[1] = zset of (dead|scheduled|retry), eg, work:dead
// ARGV[1] = died at. The z rank of the job.
// ARGV[2] = job ID to requeue
// Returns:
// - number of jobs deleted (typically 1 or 0)
// - job bytes (last job only)
var redisLuaDeleteSingleCmd = `
local jobs, i, j, deletedCount, jobBytes
jobs = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[1])
local jobCount = #jobs
jobBytes = ''
deletedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  if j['id'] == ARGV[2] then
    redis.call('zrem', KEYS[1], jobs[i])
    deletedCount = deletedCount + 1
    jobBytes = jobs[i]
  end
end
return {deletedCount, jobBytes}
`

// KEYS[1] = zset of dead jobs, eg, work:dead
// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
// ARGV[3] = died at. The z rank of the job.
// ARGV[4] = job ID to requeue
// Returns: number of jobs requeued (typically 1 or 0)
var redisLuaRequeueSingleDeadCmd = `
local jobs, i, j, queue, found, requeuedCount
jobs = redis.call('zrangebyscore', KEYS[1], ARGV[3], ARGV[3])
local jobCount = #jobs
requeuedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  if j['id'] == ARGV[4] then
    redis.call('zrem', KEYS[1], jobs[i])
    queue = ARGV[1] .. j['name']
    found = false
    for _,v in pairs(KEYS) do
      if v == queue then
        j['t'] = tonumber(ARGV[2])
        j['fails'] = nil
        j['failed_at'] = nil
        j['err'] = nil
        redis.call('lpush', queue, cjson.encode(j))
        requeuedCount = requeuedCount + 1
        found = true
        break
      end
    end
    if not found then
      j['err'] = 'unknown job when requeueing'
      j['failed_at'] = tonumber(ARGV[2])
      redis.call('zadd', KEYS[1], ARGV[2] + 5, cjson.encode(j))
    end
  end
end
return requeuedCount
`

// KEYS[1] = zset of dead jobs, eg work:dead
// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
// ARGV[3] = max number of jobs to requeue
// Returns: number of jobs requeued
var redisLuaRequeueAllDeadCmd = `
local jobs, i, j, queue, found, requeuedCount
jobs = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, ARGV[3])
local jobCount = #jobs
requeuedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  redis.call('zrem', KEYS[1], jobs[i])
  queue = ARGV[1] .. j['name']
  found = false
  for _,v in pairs(KEYS) do
    if v == queue then
      j['t'] = tonumber(ARGV[2])
      j['fails'] = nil
      j['failed_at'] = nil
      j['err'] = nil
      redis.call('lpush', queue, cjson.encode(j))
      requeuedCount = requeuedCount + 1
      found = true
      break
    end
  end
  if not found then
    j['err'] = 'unknown job when requeueing'
    j['failed_at'] = tonumber(ARGV[2])
    redis.call('zadd', KEYS[1], ARGV[2] + 5, cjson.encode(j))
  end
end
return requeuedCount
`

// KEYS[1] = job queue to push onto
// KEYS[2] = Unique job's key. Test for existence and set if we push.
// ARGV[1] = job
var redisLuaEnqueueUnique = `
if redis.call('set', KEYS[2], '1', 'NX', 'EX', '86400') then
  redis.call('lpush', KEYS[1], ARGV[1])
  return 'ok'
end
return 'dup'
`

// KEYS[1] = scheduled job queue
// KEYS[2] = Unique job's key. Test for existence and set if we push.
// ARGV[1] = job
// ARGV[2] = epoch seconds for job to be run at
var redisLuaEnqueueUniqueIn = `
if redis.call('set', KEYS[2], '1', 'NX', 'EX', '86400') then
  redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])
  return 'ok'
end
return 'dup'
`

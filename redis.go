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

func redisKeyExclusiveJobs(namespace string) string {
	return redisNamespacePrefix(namespace) + "exclusive_jobs"
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

func redisKeyJobsPaused(namespace, jobName string) string {
	return fmt.Sprintf("%s:%s", redisKeyJobs(namespace, jobName), "paused")
}

func redisKeyJobsLocked(namespace, jobName string) string {
	return fmt.Sprintf("%s:%s", redisKeyJobs(namespace, jobName), "locked")
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

// To help with usages of redisLuaRpoplpushMultiCmd
// 4 args per job + 1 arg for exclusive set (which is per namespace)
func numArgsFetchJobLuaScript(numJobTypes int) int {
	return (numJobTypes * 4) + 1
}

// KEYS[1] = the set of jobs that are to run jobs exclusively
// KEYS[2] = the 1st job queue we want to try, eg, "work:jobs:emails"
// KEYS[3] = the 1st job queue's in prog queue, eg, "work:jobs:emails:97c84119d13cb54119a38743:inprogress"
// KEYS[4] = the 1st job queue's paused key, eg, "work:jobs:emails:paused"
// KEYS[5] = the 1st job queue's lock key, e.g., "work:jobs:emails:locked"
// KEYS[6] = the 2nd job queue...
// KEYS[7] = the 2nd job queue's in prog queue...
// KEYS[8] = the 2nd job queue's paused key...
// KEYS[9] = the 2nd job queue's locked key...
// ...
// KEYS[N] = the last job queue...
// KEYS[N+1] = the last job queue's in prog queue...
// KEYS[N+2] = the last job queue's paused key...
// KEYS[N+3] = the last job queue's locked key...
var redisLuaRpoplpushMultiCmd = `
local function isPaused(p)
  return redis.call('get', p)
end

local function isLocked(l)
  return redis.call('get', l)
end

local function isExclusive(q, exc)
  return redis.call('sismember', exc, q) == 1
end

local function runExclusive(q, l, exc)
  if isExclusive(q, exc) then
    redis.call('set', l, '1')
  end
end

local res, runQueue, inProgQueue, pauseKey, lockedKey
local exclQueues = KEYS[1]
local keylen = #KEYS - 1

for i=2,keylen,4 do
  runQueue = KEYS[i]
  inProgQueue = KEYS[i+1]
  pauseKey = KEYS[i+2]
  lockedKey = KEYS[i+3]

  if not isPaused(pauseKey) and not isLocked(lockedKey) then
    res = redis.call('rpoplpush', runQueue, inProgQueue)
    if res then
      runExclusive(runQueue, lockedKey, exclQueues)
      return {res, runQueue, inProgQueue}
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
// KEYS[2] = Unique job's key. Test for existance and set if we push.
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

// KEYS[1] = jobs run queue
// KEYS[2] = job types lock key
var redisLuaCheckStaleQueueLocks = `
local function isLocked(lockedKey)
  return redis.call('get', lockedKey)
end

local function isInProgress(runQueue)
  return redis.call('keys', runQueue .. ':*:inprogress')
end

if isLocked(KEYS[2]) and next(isInProgress(KEYS[1])) == nil then
  return 1
end
return 0
`

package work

import "fmt"

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

func redisKeyWorkerStatus(namespace, workerID string) string {
	return redisNamespacePrefix(namespace) + "worker:" + workerID
}

func redisKeyWorkerPools(namespace string) string {
	return redisNamespacePrefix(namespace) + "worker_pools"
}

func redisKeyHeartbeat(namespace, workerPoolID string) string {
	return redisNamespacePrefix(namespace) + "worker_pools:" + workerPoolID
}

var redisLuaRpoplpushMultiCmd = `
local res
local keylen = #KEYS
for i=1,keylen,2 do
  res = redis.call('rpop', KEYS[i])
  if res then
    redis.call('lpush', KEYS[i+1], res)
    return {res, KEYS[i], KEYS[i+1]}
  end
end
return nil`

// KEYS[1] = zset of jobs (retry or scheduled), eg work:retry
// KEYS[2] = zset of dead, eg work:dead. If we don't know the jobName of a job, we'll put it in dead.
// KEYS[3...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
var redisLuaZremLpushCmd = `
local res, j, queue
res = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, 1)
if #res > 0 then
  j = cjson.decode(res[1]) -- TODO: if this can't be parsed, we're hosed.
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

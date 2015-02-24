package work

// returns "<namespace>:jobs:"
// so that we can just append the job name and be good to go
func redisKeyJobsPrefix(namespace string) string {
	// add a colon if it doesn't exist
	l := len(namespace)
	if (l > 0) && (namespace[l-1] != ':') {
		namespace = namespace + ":"
	}

	return namespace + "jobs:"
}

func redisKeyJobs(namespace, jobName string) string {
	return redisKeyJobsPrefix(namespace) + jobName
}

func redisKeyJobsInProgress(namespace, jobName string) string {
	return redisKeyJobs(namespace, jobName) + ":inprogress"
}


var redisLuaRpoplpushMultiCmd = `
local res
local keylen = #KEYS/2
for i=1,keylen,1 do
  res = redis.call('rpop', KEYS[i])
  if res then
    redis.call('lpush', KEYS[i+keylen], res)
    return {res, KEYS[i]}
  end
end
return nil`
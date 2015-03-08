TODO
----
 - JSON api to get basic stuff
 - middleware & reflection
 - checkin stuff
 
 - change api.go:
   - Is there a reason to separate out WorkerPoolIDs() vs WorkerPoolStatuses(workerPoolIDs []string)?
     - seems annoying.
 - If we have a lot of workers, we're facing a LOT of redis calls to get all of the status.
   - possible sol'n: don't use redis hashes, just use json. Then it's just a single multiget
 - write better tests for Enqueue and EnqueueIn
 - revisit naming convention of heartbeat shit
 - revisit the retry backoff
 - rename join to drain
 - make start, stop, start work, & make start idempotent (on the pool)
 - generally, look into process scalability. Eg, if we have 30 processes, each with concurrency=25, that's a lot of pinging redis
 - do we want to merge api and enqueuer?
 - thought: what if we *scale up* to max workers if some are idle, should we shut them down?
   - thing we're guarding against: 100 goroutines all polling redis
   - alt: some clever mechanism to only check redis if we are busy?
 - is there some way to detect redis contention

workerPool := work.NewWorkerPool(Context{}, 15, &work.WorkerOptions{Redis: redisDSN}).
    Middleware((*Context).SetDatabase).
    Middleware((*Context).Log)

workerPool.Job("create_watch", (*Context).CreateWatch)
workerPool.Job("send_notice", (*Context).SendNotice)
workerPool.JobWithOptions("send_important_notice", &JobOptions{Priority: 4, Retries: 4}, (*Context).SendImportantNotice)


workerPool.Start()
workerPool.Stop()

enqueuer := worker.Enqueuer()
// or
enqueuer := work.NewEnqueuer(redisDSN)

enqueuer.Enqueue("create_watch", user.Id, suggestion.Id)
enqueuer.EnqueueIn
enqueuer.EnqueueAt
enqueuer.SourcedEnqueue("create_watch", subdomain.Id, user.Id, suggestion.Id)
enqueuer.SourcedEnqueueIn
enqueuer.SourcedEnqueueAt

ideas:
enqueuer.EnqueueUnique("create_watch", user.id)

func (c *Context) CreateWatch(r *work.Job, userId int64, suggestionId int64) error {
    
}

type Job struct {
    Name string
    ID   string
    
    SourceID int64
    
    payload []byte //???
}

// For long running jobs, checkin periodically. Returns whether the service needs to shut down.
func (r *Job) Checkin(msg string) bool {
    
}

* NewWorker() generates a random id for itself.


// design thoughts
 - goals
   1. optimize for understandability, debuggability, instrumentation
   2. redis cxns/load
   3. thruput
   4. latency
 - JOBS ARE QUEUES. Each job is in its own queue. Scheduling is round robin, but with job priorities.
   - One config that isn't possible: "always do these jobs last" or "always do jobs in this order"
 - should never have to list keys in redis
 - If shit is going blazing fast, do we need to see what's in progress?
 - what if we just show how many threads are tackling a given job at a time?
 - each worker has a unique identifier

// enqueue:
enqueuer.SourcedEnqueue("create_watch", subdomain.Id, user.Id, suggestion.Id)
lpush <namespace>:queue:create_watch {jid: "abc", t: 14494, source: 4, args: [1, 3]}

// grab it:
msg = rpoplpush <ns>:queue:create_watch <ns>:queue:create_watch:inprogress

// success:
lrem <ns>:queue:create_watch:inprogress 1 msg

// error, retry:

// error, morgue:

// enqueue-in:

// all inprogress jobs:

// all workers (eg processes-ish)

// all queues, how many jobs are enqueued:


------------

workerpool:

one fetcher per queue, each pulling with brpoplpush
got one? ok.... we have a pool of processors
 - by virtue of brpoplpush, it IS in in-progress
 - channel per queue. Put work on channel. channel has WORKER SIZE OR 1 slots in it. so this could block of it's full
 - now, workers. workers randomly pull from a channel by priority sampling.

*** MINOR downside: we pop off more than we work on right away, so in shutdown situations we make needing to recover via the in-progress queue much bigger.
*** HUGE DOWNSIDE: need 1 redis connection per queue. This becomes really bad if you have an app with >50 jobs. If queues != jobs, it's no big deal. you can have tons of jobs but a limited # of queues. if queues == jobs, you're screwed.

for each key
  r = redis.rpop key
  if r
    redis.lpush key+":inprogress", r
    return r, key
return nil

Rails.redis.del("list1", "list2", "list3")
Rails.redis.del("list1:inprog", "list2:inprog", "list3:inprog")
Rails.redis.lpush("list1", "v1")
Rails.redis.lpush("list3", "v2")

s = "
local res
local keylen = #KEYS/2
for i=1,keylen,1 do
  res = redis.call('rpop', KEYS[i])
  if res then
    redis.call('lpush', KEYS[i+keylen], res)
    return {res, KEYS[i]}
  end
end
return nil
"
Rails.redis.del("list1", "list2", "list3")
Rails.redis.del("list1:inprog", "list2:inprog", "list3:inprog")
Rails.redis.lpush("list1", "v1")
Rails.redis.lpush("list3", "v2")
Rails.redis.eval(s, ["list1","list2","list3", "list1:inprog", "list2:inprog", "list3:inprog"])
Rails.redis.lrange("list1", 0, -1)
Rails.redis.lrange("list2", 0, -1)
Rails.redis.lrange("list3", 0, -1)
Rails.redis.lrange("list1:inprog", 0, -1)
Rails.redis.lrange("list2:inprog", 0, -1)
Rails.redis.lrange("list3:inprog", 0, -1)

t="
local res = redis.call('rpop', KEYS[1]);
if not res then
  return 'not'
end
if res == nil then
  return 'isnil'
end
if res == '' then
  return 'isblank'
end
if res == 0 then
  return 'iszero'
end
return 'wat'
"


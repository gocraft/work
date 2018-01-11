
Folk from go-work

瓜子 轻量级后台脚本分布式执行框架， 在原repo的基础上增加了多任务管理和脚本执行日报功能

###安装
```go
go get -u github.com/Guazi-inc/go-work
```

###使用方式

瓜子内部使用了部分go库，etcd（获取redis地址）,data-soup(redis client)等
```go
// Global prefix for the service
const (
	prefix = "callcron"    
)

  

var wkPoolMgr *manager.WorkerPoolManager
  

func main() {
	// Initialize global resources
	_ = bootstrap.Initialize()

	// Get redis address, used for go-work library
	var redisConf base.RedisConfig
	if err := etcd.GetBaseConfig(base.ConfigKey_redis, &redisConf); err != nil {
		log.CustomLogger.Error("Read Etcd redis config error %s", err.Error())
		return
	}
  
	
	// Create Worker pool manager, the last param: RedisClient is for daily mail report
	wkPoolMgr = manager.NewWorkerPoolManager(
		prefix,                   //prefix for each cron service instance, unique global
		redisConf.Address,        //redis address for storing job
		etcd.IsProd(),            //environment
		bootstrap.BS.RedisClient, //redisClient used for daily report , optional
		bootstrap.BS.StaffHTTP)   //staffClient for sending daily report mail, optional
  
		
	// Optional step: set concurrency works for individual namespace, default: 1  if not specified
	wkPoolMgr.SetConcurrency(configs.WORKERCONCURRENCY)
	// Optional step: set mail recipients , note: the owner for each task will be added to the mail group automatically
	wkPoolMgr.SetMailGroup(configs.DailyReportMailGroup)
  
	
	wkPoolMgr.SetDailyReportTime("07:00:00")  // customize daily report send time
	wkPoolMgr.SetDailyReportTitle("xx部门Go任务执行日报")   // customize daily report mail title
	
	// register tasks
	registerTasks()
  
	
	// Start the work!
	wkPoolMgr.Start()
}

  

// register task here
func registerTasks() {

	wkPoolMgr.RegisterPeriodicTask(&sample.SampleTask{})
	}
```

示例脚本任务
```go
type SampleTask struct {
	task.TaskBase
}


//Setup init env
func (t *SampleTask) Setup() {
	t.SetMetaData(t, "SampleNamespace", "name@sample.com", "示例任务", true)

	ts := work.NewSpec()
	ts.EveryMinutes(30)   //每30分钟执行一次
	t.AddCronTask("", ts, t.Exec)
}

//Exec worker func
func (t *SampleTask) Exec(_ *work.Job) error {
	//work logic here
	return nil
}
```
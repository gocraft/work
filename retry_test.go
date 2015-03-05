package work

//import (
//	"github.com/garyburd/redigo/redis"
//	"github.com/stretchr/testify/assert"
//	"testing"
//	"fmt"
//	// "time"
//	// "os"
//	// "log"
//)
//
//func TestRequeue(t *testing.T) {
//	pool := newTestPool(":6379")
//	ns := "work"
//	cleanKeyspace(ns, pool)
//	enqueuer := NewEnqueuer(ns, pool)
//	err := enqueuer.EnqueueIn("wat", 10, 1, "cool")
//	assert.NoError(t, err)
//
//
//
//	var args []interface{}
//	zSchedKey := redisKeyScheduled(ns)
//	args = append(args, zSchedKey)
//	args = append(args, redisKeyDead(ns))
//
//	allQueues := []string{"work:jobs:wat", "work:jobs:foo"}
//	for _, q := range allQueues {
//		args = append(args, q)
//	}
//
//	args = append(args, nowEpochSeconds() + 12)
//	args = append(args, "work:jobs:")
//
//	script := redis.NewScript(len(allQueues) + 2, redisLuaZremLpushCmd)
//	conn := pool.Get()
//	defer conn.Close()
//	val, err := redis.String(script.Do(conn, args...))
//	assert.NoError(t, err)
//	fmt.Println(val)
//
//}
//

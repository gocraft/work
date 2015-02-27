package work

import (
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"testing"
	// "fmt"
	"time"
	// "os"
	// "log"
)

func TestEnqueue(t *testing.T) {
	pool := newTestPool(":6379")
	enqueuer := NewEnqueuer("work", pool)
	err := enqueuer.Enqueue("wat", 1, "cool")

	assert.Nil(t, err)

	//
	// conn, err := redis.Dial("tcp", ":6379")
	// defer conn.Close()
	// fmt.Println("err = ", err)
	// //fmt.Println("conn = ", conn)
	//
	// conn.Do("SET", "foo", 1)
	// exists, _ := redis.Bool(conn.Do("EXISTS", "foo"))
	// fmt.Printf("%#v\n", exists)
}

func newTestPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   3,
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return c, nil
			//return redis.NewLoggingConn(c, log.New(os.Stdout, "", 0), "redis"), err
		},
		//TestOnBorrow: func(c redis.Conn, t time.Time) error {
		//	_, err := c.Do("PING")
		//	return err
		//},
	}
}

package main

import (
	"flag"
	"fmt"
	"time"
	"github.com/gocraft/work/webui"
	"github.com/garyburd/redigo/redis"
)

var redisHostPort = flag.String("redis", ":6379", "redis hostport")
var redisNamespace = flag.String("ns", "work", "redis namespace")
var webHostPort = flag.String("listen", ":5040", "hostport to listen for HTTP JSON API")

func main() {
	flag.Parse()
	
	fmt.Println("Starting workwebui:")
	fmt.Println("redis = ", *redisHostPort)
	fmt.Println("namespace = ", *redisNamespace)
	fmt.Println("listen = ", *webHostPort)
	
	pool := newPool(*redisHostPort)
	
	server := webui.NewWebUIServer(*redisNamespace, pool, *webHostPort)
	server.Start()
	
	select{}
}
 
func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   20,
		MaxIdle:     20,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		Wait: true,
	}
}
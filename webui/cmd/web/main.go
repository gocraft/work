package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gocraft/work/webui"
)

func main() {
	var (
		ns        = os.Getenv("NS")
		port      = os.Getenv("PORT")
		redisAddr = os.Getenv("REDIS_ADDR")
	)
	const (
		redisPoolSize = 100
	)

	redisPool := &redis.Pool{
		MaxActive:   redisPoolSize,
		MaxIdle:     redisPoolSize,
		IdleTimeout: 15 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisAddr)
		},
		Wait: true,
	}
	s := webui.NewServer(ns, redisPool, port)
	s.Start()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	s.Stop()
}

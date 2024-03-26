package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/gocraft/work/webui"
	"github.com/gomodule/redigo/redis"
)

var (
	connection        = flag.String("conn", "tcp", "redis hostport")
	redisHostPort     = flag.String("redis", ":6379", "redis hostport")
	redisHostPassword = flag.String("pass", "", "redis password")
	redisDatabase     = flag.String("database", "0", "redis database")
	redisNamespace    = flag.String("ns", "work", "redis namespace")
	webHostPort       = flag.String("listen", ":5040", "hostport to listen for HTTP JSON API")
)

func main() {
	flag.Parse()

	fmt.Println("Starting workwebui:")
	fmt.Println("connection = ", *connection)
	fmt.Println("redis = ", *redisHostPort)
	fmt.Println("database = ", *redisDatabase)
	fmt.Println("namespace = ", *redisNamespace)
	fmt.Println("listen = ", *webHostPort)

	database, err := strconv.Atoi(*redisDatabase)
	if err != nil {
		fmt.Printf("Error: %v is not a valid database value", *redisDatabase)
		return
	}

	pool := newPool(*connection, *redisHostPort, *redisHostPassword, database)

	server := webui.NewServer(*redisNamespace, pool, *webHostPort)
	server.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	<-c

	server.Stop()

	fmt.Println("\nQuitting...")
}

func newPool(connection, addr, password string, database int) *redis.Pool {

	dialOptions := []redis.DialOption{
		redis.DialDatabase(database),
	}

	if password != "" {
		dialOptions = append(dialOptions, redis.DialPassword(password))
	}

	return &redis.Pool{
		MaxActive:   3,
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial(connection, addr, dialOptions...)
		},
		Wait: true,
	}
}

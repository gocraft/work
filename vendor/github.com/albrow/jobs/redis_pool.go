// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

// redisPool is a thread-safe pool of redis connections. Use the Get method
// to get a new connection.
var redisPool = &redis.Pool{
	MaxIdle:     10,
	MaxActive:   0,
	IdleTimeout: 240 * time.Second,
	Dial: func() (redis.Conn, error) {
		c, err := redis.Dial(Config.Db.Network, Config.Db.Address)
		if err != nil {
			return nil, err
		}
		// If a password was provided, use the AUTH command to authenticate
		if Config.Db.Password != "" {
			_, err = c.Do("AUTH", Config.Db.Password)
			if err != nil {
				return nil, err
			}
		}
		// Connect to the appropriate database with SELECT
		if _, err := c.Do("SELECT", Config.Db.Database); err != nil {
			c.Close()
			return nil, err
		}
		return c, nil
	},
}

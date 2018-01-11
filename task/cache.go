package task

import "time"

type CronCache interface {
	SetNX(key string, val interface{}, timeout time.Duration) (bool, error)
	HSet(key, field, value string) error
	HGetAll(key string) (map[string]string, error)
	HSetNX(key, field string, value interface{}) (bool, error)
	HIncr(key string, field string) error
	HIncrBy(key string, field string, value int64) (int64, error)
	Del(key string) error
}

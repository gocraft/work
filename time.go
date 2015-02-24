package work

import (
	"time"
)

func nowEpochSeconds() int64 {
	return time.Now().Unix()
}

// convert epoch seconds to a time
func epochSecondsToTime(t int64) time.Time {
	return time.Time{}
}

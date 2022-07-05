package work

import (
	"io"
	"log"
)

// StdLogger is used to log error messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Logger is the instance of a StdLogger interface that Worker writes connection
// management events to. By default it is set to discard all log messages via
// io.Discard, but you can set it to redirect wherever you want.
var Logger StdLogger = log.New(io.Discard, "[Work] ", log.LstdFlags)

func logError(key string, err error) {
	Logger.Printf("key: %s, err: %s", key, err.Error())
}

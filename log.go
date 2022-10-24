package work

import (
	"fmt"
)

type Logger interface {
	Error(string, error)
}

var _ Logger = defaultLogger{}

type defaultLogger struct{}

func (dl defaultLogger) Error(key string, err error) {
	fmt.Printf("ERROR: %s - %s\n", key, err.Error())
}

type noopLogger struct{}

func (nl noopLogger) Error(key string, err error) {}

func logError(logger Logger, key string, err error) {
	if logger == nil {
		logger = defaultLogger{}
	}
	logger.Error(key, err)
}

package work

import (
	"fmt"
)

func logError(key string, err error) {
	fmt.Printf("ERROR: %s - %s\n", key, err.Error())
}

func logInfo(format string, a ...interface{}) {
	fmt.Printf("INFO: %s\n", fmt.Sprintf(format, a...))
}

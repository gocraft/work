package work

import (
	"fmt"
	"runtime/debug"
)

func logError(key string, err error) {
	fmt.Printf("ERROR: %s - %s\n", key, err.Error())
	debug.PrintStack()
}

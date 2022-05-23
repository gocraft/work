package work

import (
	"encoding/json"
	"fmt"
)

func logError(key string, err error) {
	fmt.Printf("ERROR: %s - %s\n", key, err.Error())
}

func logPanic(panic Error) {
	stack, err := json.Marshal(panic.Stack)
	if err != nil {
		fmt.Printf("ERROR: %s - %s\n", "runJob.panic", panic.Error())
		fmt.Printf("ERROR: %s - %s\n", "runJob.panic", err.Error())
	}
	fmt.Printf("ERROR: %s - %s\n", "runJob.panic", string(stack))
}

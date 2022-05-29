package work

// Credits: https://github.com/honeybadger-io/honeybadger-go/blob/master/error.go

import (
	"fmt"
	"reflect"
	"runtime"
	"strconv"
)

const maxFrames = 20

// Frame represent a stack frame inside of a Honeybadger backtrace.
type Frame struct {
	Number string `json:"number"`
	File   string `json:"file"`
	Method string `json:"method"`
}

// Error provides more structured information about a Go error.
type Error struct {
	err     interface{}
	Message string
	Class   string
	Stack   []*Frame
}

func (e Error) Error() string {
	return e.Message
}

func newError(thing interface{}, stackOffset int) Error {
	var err error

	switch t := thing.(type) {
	case Error:
		return t
	case error:
		err = t
	default:
		err = fmt.Errorf("%v", t)
	}

	return Error{
		err:     err,
		Message: err.Error(),
		Class:   reflect.TypeOf(err).String(),
		Stack:   generateStack(stackOffset),
	}
}

func generateStack(offset int) []*Frame {
	stack := make([]uintptr, maxFrames)
	length := runtime.Callers(2+offset, stack[:])

	frames := runtime.CallersFrames(stack[:length])
	result := make([]*Frame, 0, length)

	for {
		frame, more := frames.Next()

		result = append(result, &Frame{
			File:   frame.File,
			Number: strconv.Itoa(frame.Line),
			Method: frame.Function,
		})

		if !more {
			break
		}
	}

	return result
}

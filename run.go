package work

import (
	"fmt"
)

// returns an error if the job fails, or there's a panic, or we couldn't reflect correctly.
// if we return an error, it signals we want the job to be retried.
func runJob(ctx *Context, middleware []Middleware, jt *jobType) (returnError error) {
	currentMiddleware := 0
	maxMiddleware := len(middleware)

	var next NextMiddlewareFunc
	next = func() error {
		if currentMiddleware < maxMiddleware {
			mw := middleware[currentMiddleware]
			currentMiddleware++
			return mw(ctx, next)
		}

		return jt.Handler(ctx)
	}

	defer func() {
		if panicErr := recover(); panicErr != nil {
			// err turns out to be interface{}, of actual type "runtime.errorCString"
			// Luckily, the err sprints nicely via fmt.
			errorishError := fmt.Errorf("%v", panicErr)
			logError("runJob.panic", errorishError)
			returnError = errorishError
		}
	}()

	returnError = next()

	return
}

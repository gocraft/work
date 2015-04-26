package work

import (
	"reflect"
)

// returns an error if the job fails, or there's a panic, or we couldn't reflect correctly.
// if we return an error, it signals we want the job to be retried.
func runJob(job *Job, ctxType reflect.Type, middleware []*middlewareHandler, jt *jobType) (reflect.Value, error) {

	// run middleware
	v := reflect.New(ctxType)

	currentMiddleware := 0
	maxMiddleware := len(middleware)

	var next NextMiddlewareFunc
	next = func() error {
		if currentMiddleware < maxMiddleware {
			mw := middleware[currentMiddleware]
			currentMiddleware++
			if mw.IsGeneric {
				return mw.GenericMiddlewareHandler(job, next)
			} else {
				res := mw.DynamicMiddleware.Call([]reflect.Value{v, reflect.ValueOf(job), reflect.ValueOf(next)})
				x := res[0].Interface()
				if x == nil {
					return nil
				} else {
					return x.(error)
				}
			}
		} else {
			if jt.IsGeneric {
				err := jt.GenericHandler(job)
				return err
			} else {
				res := jt.DynamicHandler.Call([]reflect.Value{v, reflect.ValueOf(job)})
				x := res[0].Interface()
				if x == nil {
					return nil
				} else {
					return x.(error)
				}
			}
		}
		return nil
	}

	err := next()

	// catch panic

	//
	return v, err
}

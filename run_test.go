package work

import (
	// "fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestRunBasicMiddleware(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		j.SetArg("mw1", "mw1")
		return next()
	}

	mw2 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record(j.Args["mw1"].(string))
		c.record("mw2")
		return next()
	}

	mw3 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record("mw3")
		return next()
	}

	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		c.record(j.Args["a"].(string)) //j TODO: change this to j.GetStringArg("a")
		return nil
	}

	middleware := []*middlewareHandler{
		&middlewareHandler{IsGeneric: true, GenericMiddlewareHandler: mw1},
		&middlewareHandler{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw2)},
		&middlewareHandler{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw3)},
	}

	jt := &jobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
		Args: map[string]interface{}{"a": "foo"},
	}

	v, err := runJob(job, tstCtxType, middleware, jt)
	assert.NoError(t, err)
	c := v.Interface().(*tstCtx)
	assert.Equal(t, "mw1mw2mw3h1foo", c.String())
}

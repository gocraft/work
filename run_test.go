package work

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func appendToContext(ctx *Context, val interface{}) {
	existing := ctx.Get("record")
	vs := ""
	if existing != nil {
		vs = existing.(string)
	}
	vs = vs + val.(string)
	ctx.Set("record", vs)
}

func TestRunBasicMiddleware(t *testing.T) {
	mw1 := func(ctx *Context, next NextMiddlewareFunc) error {
		ctx.Job.setArg("mw1", "mw1")
		return next()
	}

	mw2 := func(ctx *Context, next NextMiddlewareFunc) error {
		appendToContext(ctx, ctx.Job.Args["mw1"])
		appendToContext(ctx, "mw2")
		return next()
	}

	mw3 := func(ctx *Context, next NextMiddlewareFunc) error {
		appendToContext(ctx, "mw3")
		return next()
	}

	h1 := func(ctx *Context) error {
		appendToContext(ctx, "h1")
		appendToContext(ctx, ctx.Job.Args["a"])
		return nil
	}

	middleware := []Middleware{mw1, mw2, mw3}

	jt := &jobType{
		Name:    "foo",
		Handler: h1,
	}

	job := &Job{
		Name: "foo",
		Args: map[string]interface{}{"a": "foo"},
	}

	ctx := NewContext(job)
	err := runJob(ctx, middleware, jt)
	assert.NoError(t, err)
	assert.Equal(t, "mw1mw2mw3h1foo", ctx.Get("record"))
}

func TestRunHandlerError(t *testing.T) {
	mw1 := func(ctx *Context, next NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(ctx *Context) error {
		appendToContext(ctx, "h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []Middleware{mw1}

	jt := &jobType{
		Name:    "foo",
		Handler: h1,
	}

	job := &Job{
		Name: "foo",
	}

	ctx := NewContext(job)
	err := runJob(ctx, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "h1_err", err.Error())

	assert.Equal(t, "h1", ctx.Get("record"))
}

func TestRunMwError(t *testing.T) {
	mw1 := func(ctx *Context, next NextMiddlewareFunc) error {
		return fmt.Errorf("mw1_err")
	}
	h1 := func(ctx *Context) error {
		appendToContext(ctx, "h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []Middleware{mw1}

	jt := &jobType{
		Name:    "foo",
		Handler: h1,
	}

	job := &Job{
		Name: "foo",
	}

	ctx := NewContext(job)
	err := runJob(ctx, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "mw1_err", err.Error())
}

func TestRunHandlerPanic(t *testing.T) {
	mw1 := func(ctx *Context, next NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(ctx *Context) error {
		appendToContext(ctx, "h1")
		panic("dayam")
	}

	middleware := []Middleware{mw1}
	jt := &jobType{
		Name:    "foo",
		Handler: h1,
	}

	job := &Job{
		Name: "foo",
	}

	ctx := NewContext(job)
	err := runJob(ctx, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}

func TestRunMiddlewarePanic(t *testing.T) {
	mw1 := func(ctx *Context, next NextMiddlewareFunc) error {
		panic("dayam")
	}
	h1 := func(ctx *Context) error {
		appendToContext(ctx, "h1")
		return nil
	}

	middleware := []Middleware{mw1}

	jt := &jobType{
		Name:    "foo",
		Handler: h1,
	}

	job := &Job{
		Name: "foo",
	}

	ctx := NewContext(job)
	err := runJob(ctx, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}

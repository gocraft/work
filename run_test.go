package work

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestRunBasicMiddleware(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		j.setArg("mw1", "mw1")
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
		c.record(j.Args["a"].(string))
		return nil
	}

	middleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw2)},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw3)},
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

func TestRunJobMiddleware(t *testing.T) {
	jmw1 := func(j *Job, next NextMiddlewareFunc) error {
		j.setArg("jmw1", "jmw1")
		return next()
	}

	jmw2 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record(j.Args["jmw1"].(string))
		c.record("jmw2")
		next()
		c.record("jmw2")
		return nil
	}

	jmw3 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record("jmw3")
		next()
		c.record("jmw3")
		return nil
	}

	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		c.record(j.Args["a"].(string))
		return nil
	}

	jobMiddleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: jmw1},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(jmw2)},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(jmw3)},
	}

	jt := &jobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
		middleware:     jobMiddleware,
	}

	job := &Job{
		Name: "foo",
		Args: map[string]interface{}{"a": "foo"},
	}

	v, err := runJob(job, tstCtxType, nil, jt)
	assert.NoError(t, err)
	c := v.Interface().(*tstCtx)
	assert.Equal(t, "jmw1jmw2jmw3h1foojmw3jmw2", c.String())
}

func TestRunBasicAndJobMiddleware(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		j.setArg("mw1", "mw1")
		return next()
	}

	mw2 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record(j.Args["mw1"].(string))
		c.record("mw2")
		next()
		c.record("mw2")
		return nil
	}

	mw3 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		next()
		c.record("mw3")
		return nil
	}

	jmw1 := func(j *Job, next NextMiddlewareFunc) error {
		j.setArg("jmw1", "jmw1")
		return next()
	}

	jmw2 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record(j.Args["jmw1"].(string))
		c.record("jmw2")
		next()
		c.record("jmw2")
		return nil
	}

	jmw3 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record("jmw3")
		return next()
	}

	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		c.record(j.Args["a"].(string))
		return nil
	}

	middleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw2)},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw3)},
	}

	jobMiddleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: jmw1},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(jmw2)},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(jmw3)},
	}

	jt := &jobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
		middleware:     jobMiddleware,
	}

	job := &Job{
		Name: "foo",
		Args: map[string]interface{}{"a": "foo"},
	}

	v, err := runJob(job, tstCtxType, middleware, jt)
	assert.NoError(t, err)
	c := v.Interface().(*tstCtx)
	assert.Equal(t, "mw1mw2jmw1jmw2jmw3h1foojmw2mw3mw2", c.String())
}

func TestRunHandlerError(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	v, err := runJob(job, tstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "h1_err", err.Error())

	c := v.Interface().(*tstCtx)
	assert.Equal(t, "h1", c.String())
}

func TestRunMwError(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		return fmt.Errorf("mw1_err")
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	_, err := runJob(job, tstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "mw1_err", err.Error())
}

func TestRunHandlerPanic(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")

		panic("dayam")
	}

	middleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	_, err := runJob(job, tstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}

func TestRunMiddlewarePanic(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		panic("dayam")
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		return nil
	}

	middleware := []*middlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	_, err := runJob(job, tstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}

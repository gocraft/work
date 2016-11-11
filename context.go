package work

type Context struct {
	Job  *Job
	data map[string]interface{}
}

func NewContext(job *Job) *Context {
	return &Context{
		Job:  job,
		data: make(map[string]interface{}),
	}
}

func (c *Context) Get(key string) interface{} {
	return c.data[key]
}

func (c *Context) Set(key string, value interface{}) {
	c.data[key] = value
}

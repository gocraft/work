package workers

type WorkersLogger interface {
	Println(...interface{})
	Printf(string, ...interface{})
}

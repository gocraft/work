package workers

var beforeStart []func()
var duringDrain []func()

func BeforeStart(f func()) {
	access.Lock()
	defer access.Unlock()
	beforeStart = append(beforeStart, f)
}

// func AfterStart
// func BeforeQuit
// func AfterQuit

func DuringDrain(f func()) {
	access.Lock()
	defer access.Unlock()
	duringDrain = append(duringDrain, f)
}

func runHooks(hooks []func()) {
	for _, f := range hooks {
		f()
	}
}

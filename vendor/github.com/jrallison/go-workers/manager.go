package workers

import (
	"strings"
	"sync"
)

type manager struct {
	queue       string
	fetch       Fetcher
	job         jobFunc
	concurrency int
	workers     []*worker
	workersM    *sync.Mutex
	confirm     chan *Msg
	stop        chan bool
	exit        chan bool
	mids        *Middlewares
	*sync.WaitGroup
}

func (m *manager) start() {
	m.Add(1)
	m.loadWorkers()
	go m.manage()
}

func (m *manager) prepare() {
	if !m.fetch.Closed() {
		m.fetch.Close()
	}
}

func (m *manager) quit() {
	Logger.Println("quitting queue", m.queueName(), "(waiting for", m.processing(), "/", len(m.workers), "workers).")
	m.prepare()

	m.workersM.Lock()
	for _, worker := range m.workers {
		worker.quit()
	}
	m.workersM.Unlock()

	m.stop <- true
	<-m.exit

	m.reset()

	m.Done()
}

func (m *manager) manage() {
	Logger.Println("processing queue", m.queueName(), "with", m.concurrency, "workers.")

	go m.fetch.Fetch()

	for {
		select {
		case message := <-m.confirm:
			m.fetch.Acknowledge(message)
		case <-m.stop:
			m.exit <- true
			break
		}
	}
}

func (m *manager) loadWorkers() {
	m.workersM.Lock()
	for i := 0; i < m.concurrency; i++ {
		m.workers[i] = newWorker(m)
		m.workers[i].start()
	}
	m.workersM.Unlock()
}

func (m *manager) processing() (count int) {
	m.workersM.Lock()
	for _, worker := range m.workers {
		if worker.processing() {
			count++
		}
	}
	m.workersM.Unlock()
	return
}

func (m *manager) queueName() string {
	return strings.Replace(m.queue, "queue:", "", 1)
}

func (m *manager) reset() {
	m.fetch = Config.Fetch(m.queue)
}

func newManager(queue string, job jobFunc, concurrency int, mids ...Action) *manager {
	var customMids *Middlewares
	if len(mids) == 0 {
		customMids = Middleware
	} else {
		customMids = NewMiddleware(Middleware.actions...)
		for _, m := range mids {
			customMids.Append(m)
		}
	}
	m := &manager{
		Config.Namespace + "queue:" + queue,
		nil,
		job,
		concurrency,
		make([]*worker, concurrency),
		&sync.Mutex{},
		make(chan *Msg),
		make(chan bool),
		make(chan bool),
		customMids,
		&sync.WaitGroup{},
	}

	m.fetch = Config.Fetch(m.queue)

	return m
}

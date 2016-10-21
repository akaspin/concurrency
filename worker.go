package concurrency

import (
	"context"
	"github.com/akaspin/supervisor"
	"time"
)

type jobRequest struct {
	ctx context.Context
	fn func()
}

type Worker struct {
	*supervisor.Control
	jobCh chan jobRequest
}

func newWorker(control *supervisor.Control) (w *Worker) {
	w = &Worker{
		Control: control,
		jobCh: make(chan jobRequest),
	}
	return
}

func (w *Worker) Open() (err error) {
	w.Open()
	go w.loop()
	return
}

func (w *Worker) Execute(ctx context.Context, fn func()) {
	select {
	case <-w.Control.Ctx().Done():
	case <-ctx.Done():
	case w.jobCh<- jobRequest{
			ctx: ctx,
			fn: fn,
		}:
	}
}

func (w *Worker) loop() {
	w.Acquire()
	defer w.Release()
	LOOP:
	for {
		select {
		case <-w.Control.Ctx().Done():
			break LOOP
		case job := <-w.jobCh:
			w.run(job)
		}
	}
}

func (w *Worker) run(job jobRequest) {
	w.Acquire()
	defer w.Release()

	jobDoneCh := make(chan struct{})

	go func() {
		defer close(jobDoneCh)
		job.fn()
		select {
		case <-w.Control.Ctx().Done():
		case <-job.ctx.Done():
		case jobDoneCh<- struct {}{}:
		}
	}()

	select {
	case <-w.Control.Ctx().Done():
	case <-job.ctx.Done():
	case <-jobDoneCh:
	}
}

type WorkerPoolConfig struct {
	Capacity int
	IdleTimeout time.Duration
	CloseTimeout time.Duration
	JobTimeout time.Duration
}

// WorkerPool uses pool of workers to execute tasks
type WorkerPool struct {
	*ResourcePool
	config WorkerPoolConfig
}

func NewWorkerPool(ctx context.Context, config WorkerPoolConfig) (p *WorkerPool) {
	p = &WorkerPool{
		config: config,
	}
	p.ResourcePool = NewResourcePool(ctx, ResourcePoolConfig{
		Capacity: config.Capacity,
		IdleTimeout: config.IdleTimeout,
		CloseTimeout: config.CloseTimeout,
		Factory: p.factory,
	})
	return
}

func (p *WorkerPool) Execute(ctx context.Context, fn func()) (err error) {
	w, err := p.Get(ctx)
	if err != nil {
		return
	}
	w.Execute(ctx, func() {
		defer p.Put(w)
		fn()
	})
	return
}

// Take worker from pool
func (p *WorkerPool) Get(ctx context.Context) (w *Worker, err error) {
	r, err := p.ResourcePool.Get(ctx)
	if err != nil {
		return
	}
	w = r.(*Worker)
	return
}

func (p *WorkerPool) factory() (r Resource, err error) {
	w := newWorker(supervisor.NewControlTimeout(p.Control.Ctx(), p.config.JobTimeout))
	err = w.Open()
	if err != nil {
		return
	}
	r = w
	return
}

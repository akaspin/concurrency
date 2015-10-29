package concurrency
import (
	"fmt"
	"time"
	"sync/atomic"
	"golang.org/x/net/context"
)

// Func to apply
type Fn func(context.Context, interface{}) (interface{}, error)

// Batch options
type BatchOptions struct {

	// Continue on errors
	IgnoreErrors bool

	// Timeout for single job. Ten minutes by default
	Timeout time.Duration
}

func DefaultBatchOptions() *BatchOptions {
	return &BatchOptions{
		Timeout: time.Minute * 10,
	}
}

func (o *BatchOptions) AllowErrors() *BatchOptions  {
	o.IgnoreErrors = true
	return o
}

func (o *BatchOptions) SetTimeout(timeout time.Duration) *BatchOptions {
	o.Timeout = timeout
	return o
}

type jobRequest struct {
	fn Fn
	arg interface{}
	resChan chan<- interface{}
	errChan chan<- error
	batchCtx context.Context
	timeout time.Duration
}

type worker struct {
	ctx context.Context
	jobChan chan *jobRequest
	busy *int32
}

func newWorker(ctx context.Context, jobChan chan *jobRequest, busy *int32) (res *worker) {
	res = &worker{
		ctx: ctx,
		jobChan: jobChan,
		busy: busy,
	}
	go res.work()
	return
}

func (w *worker) work() {
	for {
		select {
		case job := <-w.jobChan:
			atomic.AddInt32(w.busy, 1)
			if w.do(job) {
				return
			}
		case <- w.ctx.Done():
			return
		}
		atomic.AddInt32(w.busy, -1)
	}
}

func (w *worker) do(job *jobRequest) (stop bool) {
	// skip job if batch context already cancelled
	select {
	case <-job.batchCtx.Done():
		return
	default:
	}

	// make cancellable context
	var jobCtx context.Context
	var jobCancel context.CancelFunc
	if job.timeout != 0 {
		jobCtx, jobCancel = context.WithTimeout(job.batchCtx, job.timeout)
	} else {
		jobCtx, jobCancel = context.WithCancel(job.batchCtx)
	}

	resChan := make(chan interface{}, 1)
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic in worker %s", r)
			}
		}()

		if res, err := job.fn(jobCtx, job.arg); err != nil {
			errChan <- err
		} else {
			resChan <- res
		}
	}()
	select {
	case res := <-resChan:
		job.resChan <- res
	case err := <-errChan:
		job.errChan <- err
	case <- w.ctx.Done():
		jobCancel()
		stop = true
	case <-jobCtx.Done():
	}
	return
}

type BatchPool struct {
	// pool context
	ctx context.Context
	cancel context.CancelFunc

	// workers (why we need this?)
	workers []*worker

	// shared job channel
	jobChan chan *jobRequest

	// busy couter
	busy int32
}

func NewPool(size int) (res *BatchPool) {
	res = &BatchPool{
		jobChan: make(chan *jobRequest, size),
		busy: 0,
	}
	res.ctx, res.cancel = context.WithCancel(context.Background())

	for i := 0; i < size; i++ {
		res.workers = append(res.workers, newWorker(res.ctx, res.jobChan, &res.busy))
	}
	return
}

func (p *BatchPool) Do(
	fn Fn,
	in *[]interface{},
	out *[]interface{},
	options *BatchOptions,
) (err error) {
	n := len(*in)
	var errs []error

	resChan := make(chan interface{}, n)
	errChan := make(chan error, n)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, arg := range *in {
		go func(arg interface{}) {
			p.jobChan<- &jobRequest{fn, arg, resChan, errChan,
				ctx, options.Timeout}
		}(arg)
	}

	poll:
	for i := 0; i < n; i++ {
		select {
		case err1 := <-errChan:
			if !options.IgnoreErrors {
				err = err1
				break poll
			}
			errs = append(errs, err1)
		case res := <-resChan:
			if res != interface{}(nil) {
				*out = append(*out, res)
			}
		case <-ctx.Done():
			break poll
		case <-p.ctx.Done():
			break poll
		}
	}

	if len(errs) > 0 {
		err = fmt.Errorf("%s", errs)
	}
	return
}

func (p *BatchPool) Busy() int {
	return int(atomic.LoadInt32(&p.busy))
}

func (p *BatchPool) Close() {
	p.cancel()
}
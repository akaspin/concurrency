package concurrency

import (
	"io"
	"time"
	"github.com/akaspin/supervisor"
	"context"
	"errors"
)

var (
	PoolClosedError = errors.New("pool closed")
	PoolIsFullError = errors.New("pool is full")
)

type Resource interface {
	io.Closer
}

type ResourceFactory func() (Resource, error)

type resourceWrapper struct {
	resource Resource
	timeUsed time.Time
}

type ResourcePoolConfig struct {
	Capacity int
	Factory ResourceFactory

	IdleTimeout time.Duration
	CloseTimeout time.Duration

}

type ResourcePool struct {
	*supervisor.Control
	resourcesCh chan resourceWrapper
	config *ResourcePoolConfig
}

func NewResourcePool(
	ctx context.Context,
	config ResourcePoolConfig) (p *ResourcePool) {
	p = &ResourcePool{
		Control: supervisor.NewControl(ctx),
		resourcesCh: make(chan resourceWrapper, config.Capacity),
		config: config,
	}
	return
}

func (p *ResourcePool) Get(ctx context.Context) (r Resource, err error) {
	select {
	case <-p.Control.Ctx().Done():
		err = PoolClosedError
		return
	case <-ctx.Done():
		err = context.Canceled
	default:
	}

	var wrapper resourceWrapper
	var ok bool
	select {
	case wrapper, ok = <-p.resourcesCh:
	case <-p.Control.Ctx().Done():
		err = PoolClosedError
		return
	case <-ctx.Done():
		err = context.Canceled
	}
	if !ok {
		err = PoolClosedError
		return
	}

	if wrapper.resource != nil && p.config.IdleTimeout > 0 &&
			wrapper.timeUsed.Add(p.config.IdleTimeout).Sub(time.Now()) < 0 {
		wrapper.resource.Close()
		wrapper.resource = nil
	}
	if wrapper.resource == nil {
		wrapper.resource, err = p.config.Factory()
		if err != nil {
			p.resourcesCh<- resourceWrapper{}
			return
		}
	}
	r = wrapper.resource
	p.Acquire()
	return
}

func (p *ResourcePool) Put(r Resource) (err error) {
	var wrapper resourceWrapper
	if r != nil {
		wrapper = resourceWrapper{
			resource: r,
			timeUsed: time.Now(),
		}
	}
	select {
	case p.resourcesCh<- wrapper:
		p.Release()
	default:
		err = PoolIsFullError
	}
	return
}

func (p *ResourcePool) Open() (err error) {
	p.Control.Open()
	for i := 0; i < p.config.Capacity; i++ {
		p.resourcesCh<- resourceWrapper{}
	}
	return
}


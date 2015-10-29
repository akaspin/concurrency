package concurrency

import (
	"github.com/akaspin/vitess_pools"
	"golang.org/x/net/context"
	"time"
)

type Lock struct {
	pool *pools.ResourcePool
}

func (l *Lock) Release()  {
	l.pool.Put(l)
	return
}

func (l *Lock) Close() (err error) {
	return
}

// Locks is simple pool for track limited
// resources such a FD.
type Locks struct {
	pool *pools.ResourcePool
	ctx context.Context
	cancel context.CancelFunc
}

func NewLocks(ctx context.Context, n int, ttl time.Duration) (res *Locks) {
	res = &Locks{}
	res.ctx, res.cancel = context.WithCancel(ctx)
	res.pool = pools.NewResourcePool(res.factory, n, n, ttl)
	return
}

func (p *Locks) Take() (res *Lock, err error) {
	r1, err := p.pool.Get(p.ctx)
	if err != nil {
		return
	}
	res = r1.(*Lock)
	return
}

func (p *Locks) With(fn func() error) (err error) {
	l, err := p.Take()
	if err != nil {
		return
	}
	defer l.Release()
	err = fn()
	return
}

func (p *Locks) Available() int64 {
	return p.pool.Available()
}

func (p *Locks) Close() (err error) {
	p.cancel()
	p.pool.Close()
	return
}

func (p *Locks) factory() (res pools.Resource, err error) {
	res = &Lock{p.pool}
	return
}
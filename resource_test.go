package concurrency_test

import (
	"testing"
	"github.com/akaspin/concurrency"
	"context"
	"sync/atomic"
	"github.com/stretchr/testify/assert"
	"time"
)

type nopCloser struct {
	closeCounter *int64
}

func (n *nopCloser) Close() (err error) {
	atomic.AddInt64(n.closeCounter, 1)
	return
}

func TestNewResourcePool(t *testing.T) {
	var factoryCount, closeCount int64
	type resource struct {
		Close func() (err error)
	}

	p := concurrency.NewResourcePool(context.TODO(), concurrency.ResourcePoolConfig{
		Capacity: 16,
		CloseTimeout: time.Millisecond * 100,
		Factory: func() (r concurrency.Resource, err error) {
			r = &nopCloser{&closeCount}
			atomic.AddInt64(&factoryCount, 1)
			return
		},
	})
	p.Open()


	r, err := p.Get(context.TODO())
	assert.NoError(t, err)

	err = p.Put(r)
	assert.NoError(t, err)

	assert.Equal(t, factoryCount, int64(1))
	assert.Equal(t, closeCount, int64(0))

	p.Close()
	err = p.Wait()
	
	assert.NoError(t, err)
	assert.Equal(t, factoryCount, int64(1))
	assert.Equal(t, closeCount, int64(1))

}

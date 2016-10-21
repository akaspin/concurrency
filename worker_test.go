package concurrency_test

import (
	"testing"
	"sync/atomic"
	"github.com/akaspin/concurrency"
	"context"
	"time"
	"github.com/stretchr/testify/assert"
	"sync"
)


func TestNewWorkerPool(t *testing.T) {
	var count int64

	fn := func() {
		atomic.AddInt64(&count, 1)
	}

	p := concurrency.NewWorkerPool(context.TODO(), concurrency.Config{
		CloseTimeout: time.Millisecond * 200,
		Capacity: 16,
	})
	err := p.Open()
	assert.NoError(t, err)

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		err = p.Execute(context.TODO(), func() {
			defer wg.Done()
			fn()
		})
	}

	wg.Wait()

	err = p.Close()
	assert.NoError(t, err)

	err = p.Wait()
	assert.NoError(t, err)

	assert.EqualValues(t, 100, count)

}

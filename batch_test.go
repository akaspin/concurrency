package concurrency_test
import (
	"testing"
	"math/rand"
	"time"
	"fmt"
	"github.com/akaspin/concurrency"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func Test_Batch_1(t *testing.T) {
	n := 100

	var req, res []interface{}
	for i := 0; i < n; i++ {
		req = append(req, i)
	}

	pool := concurrency.NewPool(16)
	defer pool.Close()

	err := pool.Do(
		func(ctx context.Context, in interface{}) (out interface{}, err error) {
			out = fmt.Sprintf("%d", in.(int))
			return
		},
		&req, &res,
		concurrency.DefaultBatchOptions(),
	)
	assert.NoError(t, err)
	assert.Len(t, res, 100)
}

func Test_Batch_Panic(t *testing.T) {
	n := 2

	var req []interface{}
	var res []interface{}
	for i := 0; i < n; i++ {
		req = append(req, i)
	}

	pool := concurrency.NewPool(16)
	defer pool.Close()
	err := pool.Do(
		func(ctx context.Context, in interface{}) (out interface{}, err error) {
			panic("BANG")
			out = fmt.Sprintf("%d", in.(int))
			return
		},
		&req,
		&res,
		concurrency.DefaultBatchOptions(),
	)
	assert.Error(t, err)
	assert.NotEqual(t, len(res), 1)
}

func Test_Batch_Error(t *testing.T)  {
	n := 100

	var req []interface{}
	var res []interface{}
	for i := 0; i < n; i++ {
		req = append(req, i)
	}

	pool := concurrency.NewPool(16)
	defer pool.Close()
	err := pool.Do(
		func(ctx context.Context, in interface{}) (out interface{}, err error) {
			if in.(int) >= 0 && in.(int) < 30 {
				err = fmt.Errorf("test error")
				return
			}
			out = fmt.Sprintf("%d", in.(int))
			return
		},
		&req,
		&res,
		concurrency.DefaultBatchOptions(),
	)
	assert.Error(t, err)
	assert.NotEqual(t, len(res), 100)
}

func Test_Batch_IgnoreErrors(t *testing.T) {
	n := 100

	var req []interface{}
	var res []interface{}
	for i := 0; i < n; i++ {
		req = append(req, i)
	}

	pool := concurrency.NewPool(16)
	defer pool.Close()
	err := pool.Do(
		func(ctx context.Context, in interface{}) (out interface{}, err error) {
			if in.(int) >= 0 && in.(int) < 30 {
				err = fmt.Errorf("test error")
				return
			}
			out = fmt.Sprintf("%d", in.(int))
			return
		},
		&req,
		&res,
		concurrency.DefaultBatchOptions().AllowErrors(),
	)
	assert.Error(t, err)
	assert.Equal(t, len(res), 70)
}

func Test_Batch_Hangs(t *testing.T) {
	n := 100

	var req []interface{}
	var res []interface{}
	for i := 0; i < n; i++ {
		req = append(req, i)
	}

	pool := concurrency.NewPool(16)
	defer pool.Close()
	time.Sleep(time.Second)
	assert.Equal(t, 0, pool.Busy())

	err := pool.Do(
		func(ctx context.Context, in interface{}) (out interface{}, err error) {
			if in.(int) == 90 {
				time.Sleep(time.Second * 5)
				return "ok", nil
			}
			return nil, fmt.Errorf("break it")
		},
		&req, &res, concurrency.DefaultBatchOptions(),
	)

	time.Sleep(time.Second)
	assert.Equal(t, 0, pool.Busy())
	assert.Error(t, err)
	assert.Equal(t, len(res), 0)
}

func op(i int) string {
	rand.Seed(time.Now().Unix())
	time.Sleep(time.Microsecond * time.Duration(rand.Int31n(1000)))
	return fmt.Sprintf("%d", i)
}
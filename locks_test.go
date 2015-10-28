package concurrency_test
import (
	"testing"
	"github.com/akaspin/concurrency"
	"golang.org/x/net/context"
	"time"
	"github.com/stretchr/testify/assert"
)

func Test_Locks(t *testing.T) {
	locks := concurrency.NewLocks(context.Background(), 100, time.Hour)
	l1, err := locks.Take()
	assert.NoError(t, err)
	assert.EqualValues(t, 99, locks.Available())

	l1.Release()

	assert.EqualValues(t, 100, locks.Available())

}
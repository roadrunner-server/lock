package lock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func devLogger() *zap.Logger {
	dl, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	return dl
}

func TestLock(t *testing.T) {
	l := NewLocker(devLogger())
	assert.True(t, l.lock("foo", "bar", 10, 0))
	time.Sleep(time.Second * 3)
	assert.True(t, l.lock("foo", "bar", 5, 10))
	go func() {
		stopCh := time.After(time.Second * 3)
		for {
			select {
			case <-stopCh:
				return
			default:
				assert.False(t, l.lock("foo", "bar", 1, 0))
			}
		}
	}()
	time.Sleep(time.Second)
}

func TestLockRelease(t *testing.T) {
	l := NewLocker(devLogger())
	assert.True(t, l.lock("foo", "bar", 10, 0))
	assert.False(t, l.lock("foo", "bar", 10, 0))

	assert.True(t, l.release("foo", "bar"))

	assert.True(t, l.lock("foo", "bar", 10, 0))
	assert.True(t, l.lock("foo", "bar", 10, 20))

	assert.True(t, l.release("foo", "bar"))
}

func TestLockReleaseMad(t *testing.T) {
	l := NewLocker(devLogger())
	stopCh := make(chan struct{})

	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				l.lock("foo", "bar", 10, 0)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				l.lock("foo", "bar", 10, 0)
			}
		}
	}()

	time.Sleep(time.Second * 10)
	stopCh <- struct{}{}
	stopCh <- struct{}{}
}

func TestLockRead(t *testing.T) {
	l := NewLocker(devLogger())
	assert.True(t, l.lockRead("foo", "bar", 1, 0))
	assert.True(t, l.lockRead("foo", "bar1", 1, 0))
	assert.True(t, l.lockRead("foo", "bar2", 1, 0))
	assert.True(t, l.lockRead("foo", "bar3", 1, 0))
	time.Sleep(time.Second * 5)
	assert.True(t, l.lockRead("foo", "bar", 1, 0))
}

func TestLockReadRelease(t *testing.T) {
	l := NewLocker(devLogger())
	assert.True(t, l.lockRead("foo", "bar", 10, 0))
	assert.True(t, l.lockRead("foo", "bar1", 10, 0))
	assert.True(t, l.lockRead("foo", "bar2", 10, 0))
	assert.True(t, l.lockRead("foo", "bar3", 10, 0))

	assert.True(t, l.release("foo", "bar"))
	assert.True(t, l.release("foo", "bar1"))
	assert.True(t, l.release("foo", "bar2"))
	assert.True(t, l.release("foo", "bar3"))
}

func TestLockUpdateTTL(t *testing.T) {
	l := NewLocker(devLogger())
	assert.True(t, l.lock("foo", "bar", 10, 0))
	wg := sync.WaitGroup{}
	wg.Add(10)
	go func() {
		for i := 0; i < 10; i++ {
			assert.True(t, l.updateTTL("foo", "bar", 5))
			time.Sleep(time.Second)
			wg.Done()
		}
	}()

	time.Sleep(time.Second)
	assert.True(t, l.lock("foo", "bar", 5, 1000))
	assert.True(t, l.release("foo", "bar"))
	wg.Wait()
}

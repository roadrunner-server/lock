package lock

import (
	"context"
)

type reslock struct {
	ch chan struct{}
}

func newResLock() *reslock {
	rl := &reslock{
		ch: make(chan struct{}, 1),
	}

	// arm
	rl.ch <- struct{}{}
	return rl
}

func (r *reslock) lock(ctx context.Context) bool {
	select {
	case <-r.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *reslock) unlock() {
	select {
	case r.ch <- struct{}{}:
	default:
		break
	}
}

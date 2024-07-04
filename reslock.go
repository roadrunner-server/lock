package lock

import (
	"context"

	"go.uber.org/zap"
)

type reslock struct {
	log *zap.Logger
	// mutex with timeout based on a channel
	operationMu chan struct{}
	// lock methods should prevent calling release method, and at the same time
	// release should be allowed only on the safe spots, e.g., waiting on notification
	releaseMu chan struct{}
}

func newResLock(log *zap.Logger) *reslock {
	rl := &reslock{
		log: log,
		// operation - lock/readLock/Release/UpdateTTL
		operationMu: make(chan struct{}, 1),
		// Should be freed to Release lock, updateTTL
		releaseMu: make(chan struct{}, 1),
	}

	// arm
	rl.operationMu <- struct{}{}
	rl.releaseMu <- struct{}{}
	return rl
}

func (r *reslock) lock(ctx context.Context) bool {
	select {
	case <-r.operationMu:
		select {
		case <-r.releaseMu:
		case <-ctx.Done():
			select {
			case r.operationMu <- struct{}{}:
			default:
				r.log.DPanic("failed to put operation semaphore back, channel is full")
			}
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *reslock) unlock() {
	select {
	case r.operationMu <- struct{}{}:
		select {
		case r.releaseMu <- struct{}{}:
		default:
			r.log.DPanic("failed to put release semaphore back, channel is full")
		}
	default:
		r.log.DPanic("failed to put operation semaphore back, channel is full")
	}
}

func (r *reslock) unlockOperation() {
	select {
	case r.operationMu <- struct{}{}:
	default:
		r.log.DPanic("failed to put operation semaphore back, channel is full")
	}
}

func (r *reslock) unlockRelease() {
	select {
	case r.releaseMu <- struct{}{}:
	default:
		r.log.DPanic("failed to put release semaphore back, channel is full")
	}
}

func (r *reslock) lockRelease(ctx context.Context) bool {
	select {
	case <-r.releaseMu:
		return true
	case <-ctx.Done():
		return false
	}
}

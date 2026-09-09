package lock

import (
	"context"
	"time"
)

const defaultImmediateTimeout = time.Millisecond

type memoryBackend struct {
	locker *locker
}

func waitContext(parent context.Context, waitUs int64) (context.Context, context.CancelFunc) {
	if waitUs == 0 {
		return context.WithTimeout(parent, defaultImmediateTimeout)
	}
	return context.WithTimeout(parent, time.Microsecond*time.Duration(waitUs))
}

func (m *memoryBackend) lock(ctx context.Context, res, id string, ttl, wait int64) (bool, error) {
	ctx, cancel := waitContext(ctx, wait)
	defer cancel()
	return m.locker.lock(ctx, res, id, int(ttl)), nil
}

func (m *memoryBackend) lockRead(ctx context.Context, res, id string, ttl, wait int64) (bool, error) {
	ctx, cancel := waitContext(ctx, wait)
	defer cancel()
	return m.locker.lockRead(ctx, res, id, int(ttl)), nil
}

func (m *memoryBackend) release(ctx context.Context, res, id string, wait int64) (bool, error) {
	ctx, cancel := waitContext(ctx, wait)
	defer cancel()
	return m.locker.release(ctx, res, id), nil
}

func (m *memoryBackend) forceRelease(ctx context.Context, res string, wait int64) (bool, error) {
	ctx, cancel := waitContext(ctx, wait)
	defer cancel()
	return m.locker.forceRelease(ctx, res), nil
}

func (m *memoryBackend) exists(ctx context.Context, res, id string, wait int64) (bool, error) {
	ctx, cancel := waitContext(ctx, wait)
	defer cancel()
	return m.locker.exists(ctx, res, id), nil
}

func (m *memoryBackend) updateTTL(ctx context.Context, res, id string, ttl, wait int64) (bool, error) {
	ctx, cancel := waitContext(ctx, wait)
	defer cancel()
	return m.locker.updateTTL(ctx, res, id, int(ttl)), nil
}

func (m *memoryBackend) stop(ctx context.Context) error {
	m.locker.stop(ctx)
	return nil
}

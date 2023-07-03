package lock

import (
	"context"

	"go.uber.org/zap"
)

func (l *locker) internalReleaseLock(ctx context.Context, id string) bool {
	select {
	case <-l.releaseMuCh:
		l.log.Debug("acquired release mutex", zap.String("id", id))
		return true
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock", zap.String("id", id))
		return false
	}
}

func (l *locker) internalReleaseUnLock(id string) {
	select {
	case l.releaseMuCh <- struct{}{}:
		l.log.Debug("releaseMuCh lock returned", zap.String("id", id))
	default:
		l.log.Debug("releaseMuCh lock not returned, channel is full", zap.String("id", id))
		break
	}
}

func (l *locker) internalLock(ctx context.Context, id string) bool {
	// only 1 goroutine might be passed here at the time
	// lock with timeout
	// todo(rustatian): move to a function
	select {
	case <-l.muCh:
		// hold release mutex as well
		select {
		case <-l.releaseMuCh:
			l.log.Debug("acquired lock and release mutexes", zap.String("id", id))
		case <-ctx.Done():
			l.log.Warn("timeout exceeded, failed to acquire releaseMuCh", zap.String("id", id))
			// we should return previously acquired lock mutex
			select {
			case l.muCh <- struct{}{}:
				l.log.Debug("muCh lock returned", zap.String("id", id))
			default:
				l.log.Debug("muCh lock not returnen, channel is full", zap.String("id", id))
			}

			return false
		}
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock", zap.String("id", id))
		return false
	}

	return true
}

func (l *locker) internalUnLock(id string) {
	l.muCh <- struct{}{}

	select {
	case l.releaseMuCh <- struct{}{}:
		l.log.Debug("releaseMuCh lock returned", zap.String("id", id))
	default:
		l.log.Debug("releaseMuCh lock not returned, channel is full", zap.String("id", id))
	}
}

func (l *locker) stop(ctx context.Context) {
	l.log.Debug("received stop signal, acquiring lock/release mutexes")
	if !l.internalLock(ctx, "") {
		return
	}

	l.log.Debug("acquired stop mutex")

	// release all mutexes
	l.resources.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(*resource)

		close(v.stopCh)
		l.log.Debug("closed broadcast channed", zap.String("id", k))
		return true
	})

	l.internalUnLock("")

	l.log.Debug("signal sent to all resources")
}

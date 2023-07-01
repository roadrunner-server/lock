package lock

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// callback function is a function which should be executed right after it inserted into hashmap
// generally callback is responsible of the removing itself from the hashmap
type callback func(id string, notifiCh chan<- struct{}, stopCh <-chan struct{})

type item struct {
	callback callback
	stopCh   chan struct{}
}
type resourceC map[string]*item

// resource
type resource struct {
	// mutex is responsible for the locks, callback safety
	// mutex is allocated per-resource
	mu sync.Mutex
	// lock is the exclusive lock
	writer atomic.Uint64
	// number of readers
	readerCount atomic.Uint64

	// queue with locks
	// might be 1 lock in case of lock or multiply in case of RLock
	// first map holds resource
	// second - associated with the resource callbacks
	locks resourceC

	// notificationCh used to notify that all locks are expired and user is free to obtain a new one
	// this channel receive an event only if there are no locks (write/read)
	notificationCh chan struct{}
	// stopCh should not receive any events. It's used as a brodcast-on-close event to notify all existing locks to expire
	stopCh chan struct{}
}

type locker struct {
	// logger
	log *zap.Logger
	// stop all, close it to broadcast stop signal
	stopCh chan struct{}
	// mutex with tiemout based on channel
	muCh chan struct{}
	// lock methods should prevent calling release method and the same time
	// release should be allowed only on the safe spots, e.g. waiting on notification
	releaseMuCh chan struct{}
	// all resources stored here
	resources map[string]*resource
}

func newLocker(log *zap.Logger) *locker {
	l := &locker{
		log:         log,
		muCh:        make(chan struct{}, 1),
		stopCh:      make(chan struct{}, 1),
		resources:   make(map[string]*resource, 5),
		releaseMuCh: make(chan struct{}, 1),
	}
	l.muCh <- struct{}{}
	l.releaseMuCh <- struct{}{}

	return l
}

// lock used to acquire exclusive lock for the resource or promote read-lock to lock with the same ID
/*
Here may have the following scenarious:
- No resource associated with the resource ID, create resource, add callback if we have TTL, increate writers to 1.
- Resource exists, no locks associated with it -> add callback if we have TTL, increase writers to 1
- Resource exists, write lock already acquired. Wait on context and notification channel. Notification will be sent when the last lock is released.

*/
func (l *locker) lock(ctx context.Context, res, id string, ttl int) bool {
	// only 1 goroutine might be passed here at the time
	// lock with timeout
	// todo(rustatian): move to a function
	select {
	case <-l.muCh:
		// hold release mutex as well
		<-l.releaseMuCh
		l.log.Debug("acquiring plugin's mutex")
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}

	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash
	if _, ok := l.resources[res]; !ok {
		l.log.Debug("no such resource, creating new",
			zap.String("id", id),
			zap.Int("ttl microseconds", ttl),
		)

		r := &resource{
			mu:             sync.Mutex{},
			notificationCh: make(chan struct{}, 1),
			stopCh:         make(chan struct{}, 1),
			locks:          make(resourceC, 1),
		}
		r.writer.Store(1)
		r.readerCount.Store(0)
		l.resources[res] = r

		if ttl == 0 {
			// no need to use callback if we don't have timeout
			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return true
		}

		// user requested lock, check it by ID
		if _, ok := r.locks[id]; ok {
			l.log.Warn("id already exists", zap.String("id", id))
			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return false
		}

		stopCbCh := make(chan struct{})
		// at this point, when adding lock, we should not have the callback
		callb := func(lockID string, notifCh chan<- struct{}, sCh <-chan struct{}) {
			// channel to forcibly remove the lock
			ta := time.After(time.Microsecond * time.Duration(ttl))
			// save the stop channel
			select {
			case <-ta:
				l.log.Debug("lock: ttl expired",
					zap.String("id", lockID),
					zap.Int("ttl microseconds", ttl),
				)
			case <-sCh:
				l.log.Debug("lock: ttl removed, broadcast call",
					zap.String("id", lockID),
					zap.Int("ttl microseconds", ttl),
				)

			case <-stopCbCh:
				l.log.Debug("lock: ttl removed, callback call",
					zap.String("id", lockID),
					zap.Int("ttl microseconds", ttl),
				)
			}

			r.mu.Lock()
			delete(r.locks, id)
			// we also have to check readers and writers
			if r.writer.Load() == 1 || r.readerCount.Load() == 1 {
				// only one resource, remove it
				// and send notification to the notification channel
				l.log.Debug("deleting the last lock, sending notification")
				notifCh <- struct{}{}
			}
			r.mu.Unlock()
			if r.writer.Load() == 1 {
				r.writer.Store(0)
				r.readerCount.Store(0)
			}

			if r.readerCount.Load() > 0 {
				// reduce number of readers
				r.readerCount.Add(^uint64(0))
			}
		}

		r.mu.Lock()
		r.locks[id] = &item{
			callback: callb,
			stopCh:   stopCbCh,
		}
		r.mu.Unlock()

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		l.muCh <- struct{}{}
		l.releaseMuCh <- struct{}{}
		return true
	}

	r := l.resources[res]

	// we don't have any lock associated with this resource
	switch {
	// we have writer
	case r.readerCount.Load() == 0 && r.writer.Load() == 1: // case when we have only readers, should also check if there are only 1 reader and it's our reader
		l.log.Debug("waiting to hold the mutex")
		// allow releasing mutexes
		l.releaseMuCh <- struct{}{}
		select {
		case <-r.notificationCh:
			// get release mutex back
			<-l.releaseMuCh

			l.log.Debug("got notification")
			r.mu.Lock()
			fmt.Println(r.locks)
			r.mu.Unlock()
			println(r.writer.Load())
			println(r.readerCount.Load())

			stopCbCh := make(chan struct{})
			// at this point, when adding lock, we should not have the callback
			callb := func(lockID string, notifCh chan<- struct{}, sCh <-chan struct{}) {
				// channel to forcibly remove the lock
				ta := time.After(time.Microsecond * time.Duration(ttl))
				// save the stop channel
				select {
				case <-ta:
					l.log.Debug("lock: ttl expired",
						zap.String("id", lockID),
						zap.Int("ttl microseconds", ttl),
					)
				case <-sCh:
					l.log.Debug("lock: ttl removed, broadcast call",
						zap.String("id", lockID),
						zap.Int("ttl microseconds", ttl),
					)

				case <-stopCbCh:
					l.log.Debug("lock: ttl removed, callback call",
						zap.String("id", lockID),
						zap.Int("ttl microseconds", ttl),
					)
				}

				r.mu.Lock()
				delete(r.locks, id)
				// we also have to check readers and writers
				if r.writer.Load() == 1 || r.readerCount.Load() == 1 {
					// only one resource, remove it
					// and send notification to the notification channel
					l.log.Debug("deleting the last lock, sending notification")
					notifCh <- struct{}{}
				}

				r.mu.Unlock()
				if r.writer.Load() == 1 {
					r.writer.Store(0)
					r.readerCount.Store(0)
				}

				if r.readerCount.Load() > 0 {
					// reduce number of readers
					r.readerCount.Add(^uint64(0))
				}
			}

			r.mu.Lock()
			r.locks[id] = &item{
				callback: callb,
				stopCh:   stopCbCh,
			}
			r.mu.Unlock()

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return true
		case <-ctx.Done():
			l.log.Debug("expired")
		}

	case r.readerCount.Load() > 0 && r.writer.Load() == 0:
		l.log.Debug("only readers")
		if r.readerCount.Load() == 1 {
			r.mu.Lock()
			if _, ok := r.locks[id]; ok {
				// promote lock from read to write
				r.stopCh <- struct{}{}
			}
			r.mu.Unlock()
			// store writer and remove reader
			r.writer.Store(1)
			r.readerCount.Store(0)

			if ttl == 0 {
				// no need to use callback if we don't have timeout
				l.muCh <- struct{}{}
				l.releaseMuCh <- struct{}{}
				return true
			}

			stopCbCh := make(chan struct{})
			// at this point, when adding lock, we should not have the callback
			callb := func(lockID string, notifCh chan<- struct{}, sCh <-chan struct{}) {
				// channel to forcibly remove the lock
				ta := time.After(time.Microsecond * time.Duration(ttl))
				// save the stop channel
				select {
				case <-ta:
					l.log.Debug("lock: ttl expired",
						zap.String("id", lockID),
						zap.Int("ttl microseconds", ttl),
					)
				case <-sCh:
					l.log.Debug("lock: ttl removed, broadcast call",
						zap.String("id", lockID),
						zap.Int("ttl microseconds", ttl),
					)

				case <-stopCbCh:
					l.log.Debug("lock: ttl removed, callback call",
						zap.String("id", lockID),
						zap.Int("ttl microseconds", ttl),
					)
				}

				r.mu.Lock()
				delete(r.locks, id)
				// we also have to check readers and writers
				if r.writer.Load() == 1 || r.readerCount.Load() == 1 {
					// only one resource, remove it
					// and send notification to the notification channel
					l.log.Debug("deleting the last lock, sending notification")
					notifCh <- struct{}{}
				}
				r.mu.Unlock()

				if r.writer.Load() == 1 {
					r.writer.Store(0)
					r.readerCount.Store(0)
				}

				if r.readerCount.Load() > 0 {
					// reduce number of readers
					r.readerCount.Add(^uint64(0))
				}
			}

			r.mu.Lock()
			r.locks[id] = &item{
				callback: callb,
				stopCh:   stopCbCh,
			}
			r.mu.Unlock()

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return true
		}

		// at this point we know, that this is not ours mutexLocked

		l.log.Debug("waiting for the mutex to unlock")
		select {
		// we've got notification, that noone holding this mutex anymore
		case <-r.notificationCh:
			// timeout exceeded
		case <-ctx.Done():
			return false
		}

	case r.readerCount.Load() == 0 && r.writer.Load() == 0:
		// drain notifications channel, just to be sure
		select {
		case <-r.notificationCh:
		default:
			break
		}

		// double check the ID
		r.mu.Lock()
		if _, ok := r.locks[id]; ok {
			l.log.Warn("id already exists", zap.String("id", id))
			r.mu.Unlock()
			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return false
		}
		r.mu.Unlock()

		// store the writer
		r.writer.Store(1)
		r.readerCount.Store(0)

		if ttl == 0 {
			// no need to use callback if we don't have timeout
			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return true
		}

		stopCbCh := make(chan struct{})
		// at this point, when adding lock, we should not have the callback
		callb := func(lockID string, notifCh chan<- struct{}, sCh <-chan struct{}) {
			// channel to forcibly remove the lock
			ta := time.After(time.Microsecond * time.Duration(ttl))
			// save the stop channel
			select {
			case <-ta:
				l.log.Debug("lock: ttl expired",
					zap.String("id", lockID),
					zap.Int("ttl microseconds", ttl),
				)
			case <-sCh:
				l.log.Debug("lock: ttl removed, broadcast call",
					zap.String("id", lockID),
					zap.Int("ttl microseconds", ttl),
				)

			case <-stopCbCh:
				l.log.Debug("lock: ttl removed, callback call",
					zap.String("id", lockID),
					zap.Int("ttl microseconds", ttl),
				)
			}

			r.mu.Lock()
			delete(r.locks, id)
			// we also have to check readers and writers
			if r.writer.Load() == 1 || r.readerCount.Load() == 1 {
				// only one resource, remove it
				// and send notification to the notification channel
				l.log.Debug("deleting the last lock, sending notification")
				notifCh <- struct{}{}
			}

			r.mu.Unlock()

			if r.writer.Load() == 1 {
				r.writer.Store(0)
				r.readerCount.Store(0)
			}

			if r.readerCount.Load() > 0 {
				// reduce number of readers
				r.readerCount.Add(^uint64(0))
			}
		}

		r.mu.Lock()
		r.locks[id] = &item{
			callback: callb,
			stopCh:   stopCbCh,
		}
		r.mu.Unlock()

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		l.muCh <- struct{}{}
		l.releaseMuCh <- struct{}{}
		return true
	}

	l.muCh <- struct{}{}
	l.releaseMuCh <- struct{}{}
	return false
}

func (l *locker) lockRead(ctx context.Context, res, id string, ttl int) bool {
	select {
	case <-l.muCh:
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.muCh <- struct{}{}
	}()

	return false
}

func (l *locker) release(ctx context.Context, res, id string) bool {
	select {
	case <-l.releaseMuCh:
		l.log.Debug("acquired release mutex", zap.String("resource", res), zap.String("id", id))
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.releaseMuCh <- struct{}{}
	}()

	if _, ok := l.resources[res]; !ok {
		l.log.Warn("no such resource", zap.String("resource", res), zap.String("id", id))
		return false
	}

	if _, ok := l.resources[res].locks[id]; !ok {
		l.log.Warn("no such resource ID", zap.String("resource", res), zap.String("id", id))
		return false
	}

	rs := l.resources[res]
	rs.mu.Lock()
	// notify close
	close(rs.locks[id].stopCh)

	rs.mu.Unlock()

	l.log.Debug("lock successfully released", zap.String("id", id))
	return true
}

func (l *locker) forceRelease(ctx context.Context, res string) bool {
	select {
	case <-l.releaseMuCh:
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.releaseMuCh <- struct{}{}
	}()

	if _, ok := l.resources[res]; !ok {
		l.log.Warn("no such resource", zap.String("resource", res))
		return false
	}

	r := l.resources[res]
	r.mu.Lock()
	close(r.stopCh)
	r.mu.Unlock()

	return true
}

func (l *locker) exists(ctx context.Context, res, id string) bool {
	select {
	case <-l.muCh:
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.muCh <- struct{}{}
	}()

	return false
}

func (l *locker) updateTTL(ctx context.Context, res, id string, ttl int) bool {
	select {
	case <-l.muCh:
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.muCh <- struct{}{}
	}()

	return true
}

func (l *locker) stop() {
	l.stopCh <- struct{}{}
}

func ptrTo[T any](val T) *T {
	return &val
}

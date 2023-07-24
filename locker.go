package lock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// callback function is a function which should be executed right after it inserted into hashmap
// generally callback is responsible of the removing itself from the hashmap
type callback func(id string, notifiCh chan<- struct{}, stopCh <-chan struct{})

// item represents callback element
type item struct {
	// callback to remove the item
	callback callback
	// item's stop channel
	stopCh chan struct{}
	// item's update TTL channel
	updateTTLCh chan int
}

// resource
type resource struct {
	// mutex is responsible for the locks, callback safety
	// mutex is allocated per-resource
	mu sync.Mutex
	// lock is the exclusive lock (should be 1 or 0)
	writerCount atomic.Uint64
	// readerCount is the number of readers (writers must be 0)
	readerCount atomic.Uint64

	// queue with locks
	// might be 1 lock in case of lock or multiply in case of RLock
	// first map holds resource
	// second - associated with the resource callbacks
	locks sync.Map // map[string]*item

	// notificationCh used to notify that all locks are expired and user is free to obtain a new one
	// this channel receive an event only if there are no locks (write/read)
	notificationCh chan struct{}
	// stopCh should not receive any events. It's used as a brodcast-on-close event to notify all existing locks to expire
	stopCh chan struct{}
}

type locker struct {
	// logger
	log *zap.Logger
	// mutex with tiemout based on channel
	muCh chan struct{}
	// lock methods should prevent calling release method and the same time
	// release should be allowed only on the safe spots, e.g. waiting on notification
	releaseMuCh chan struct{}
	// all resources stored here
	resources sync.Map // map[string]*resource
}

func newLocker(log *zap.Logger) *locker {
	l := &locker{
		log:         log,
		muCh:        make(chan struct{}, 1),
		releaseMuCh: make(chan struct{}, 1),
		resources:   sync.Map{}, // make(map[string]*resource, 5),
	}
	// init
	l.muCh <- struct{}{}
	l.releaseMuCh <- struct{}{}

	return l
}

// lock used to acquire exclusive lock for the resource or promote read-lock to lock with the same ID
/*
Here may have the following scenarios:
- No resource associated with the resource ID, create resource, add callback if we have TTL, increate writers to 1.
- Resource exists, no locks associated with it -> add callback if we have TTL, increase writers to 1
- Resource exists, write lock already acquired. Wait on context and notification channel. Notification will be sent when the last lock is released.

*/
func (l *locker) lock(ctx context.Context, res, id string, ttl int) bool {
	// only 1 goroutine might be passed here at the time
	if !l.internalLock(ctx, id, res) {
		return false
	}

	defer l.internalUnLock(id, res)

	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash
	if _, ok := l.resources.Load(res); !ok {
		l.log.Debug("no such resource, creating new",
			zap.String("resource", res),
			zap.String("id", id),
			zap.Int("ttl microseconds", ttl),
		)

		r := &resource{
			mu:             sync.Mutex{},
			notificationCh: make(chan struct{}, 1),
			stopCh:         make(chan struct{}, 1),
			locks:          sync.Map{},
		}

		r.writerCount.Store(1)
		r.readerCount.Store(0)

		l.resources.Store(res, r)

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		return true
	}

	// get resource
	// at this point both, lock and release lock should be acquired
	rr, _ := l.resources.Load(res)
	r := rr.(*resource)

	// we don't have any lock associated with this resource
	switch {
	// we have writer
	case r.readerCount.Load() == 0 && r.writerCount.Load() == 1:
		l.log.Debug("waiting to hold the mutex", zap.String("id", id))

		// here we allowing to release mutexes, because some of them might not have TTL and should be released manually
		select {
		case l.releaseMuCh <- struct{}{}:
			l.log.Debug("returning releaseMuCh mutex to temporarily allow releasing locks",
				zap.String("resource", res),
				zap.String("id", id))
		default:
			l.log.DPanic("releaseMuCh is full, skipping releaseMuCh updating",
				zap.String("resource", res),
				zap.String("id", id))
		}

		select {
		case <-r.notificationCh:
			l.log.Debug("all writers was removed, received notification", zap.String("id", id))
			// get release mutex back
			<-l.releaseMuCh
			l.log.Debug("got release mutex back", zap.String("id", id))

			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers",
					zap.String("resource", res),
					zap.String("id", id),
					zap.Uint64("writers", r.writerCount.Load()),
					zap.Uint64("readers", r.readerCount.Load()))
				return false
			}

			r.writerCount.Store(1)
			r.readerCount.Store(0)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.locks.Store(id, &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})
			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			return true
		case <-ctx.Done():
			l.log.Warn("lock notification wait timeout expired", zap.String("id", id))
			return false
		}

	case r.writerCount.Load() == 0 && r.readerCount.Load() > 0:
		// check if that's possible to elevate read lock permission to write
		if r.readerCount.Load() == 1 {
			l.log.Debug("checking readers to elevate rlock permissions, w==0, r>0", zap.String("id", id))

			rr, ok := r.locks.Load(id)
			if ok {
				// promote lock from read to write
				l.log.Debug("found read lock which can be promoted to write lock", zap.String("id", id))
				// send stop signal to the particular lock
				close(rr.(*item).stopCh)
			}

			// wait for the notification
			l.log.Debug("waiting for the notification r==1", zap.String("id", id))
			<-r.notificationCh
			l.log.Debug("r==1 notification received", zap.String("id", id))

			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers",
					zap.String("resource", res),
					zap.String("id", id),
					zap.Uint64("writers", r.writerCount.Load()),
					zap.Uint64("readers", r.readerCount.Load()))
				return false
			}

			// store writer and remove reader
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			// callback
			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.locks.Store(id, &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			return true
		}

		// at this point we know, that we have more than 1 read lock, so, we can't promote them to lock
		l.log.Debug("waiting for the readlocks to expire/release, w==0, r>0",
			zap.String("resource", res),
			zap.String("id", id))
		select {
		// we've got notification, that noone holding this mutex anymore
		case <-r.notificationCh:
			l.log.Debug("no readers holding mutex anymore, proceeding with acquiring write lock", zap.String("id", id))
			// store writer and remove reader
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			// callback
			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.locks.Store(id, &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			return true
			// timeout exceeded
		case <-ctx.Done():
			l.log.Warn("timeout expired, no lock acquired",
				zap.String("id", id),
				zap.String("resource", res))
			return false
		}

	case r.readerCount.Load() == 0 && r.writerCount.Load() == 0:
		l.log.Debug("acquiring lock, w==0, r==0",
			zap.String("resource", res),
			zap.String("id", id))
		// drain notifications channel, just to be sure
		select {
		case <-r.notificationCh:
		default:
			break
		}

		// store the writer
		r.writerCount.Store(1)
		r.readerCount.Store(0)

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		return true
	default:
		l.log.Error("unknown readlock state",
			zap.Uint64("writers", r.writerCount.Load()),
			zap.Uint64("readers", r.readerCount.Load()),
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}
}

func (l *locker) lockRead(ctx context.Context, res, id string, ttl int) bool {
	// only 1 goroutine might be passed here at the time
	// lock with timeout
	// todo(rustatian): move to a function
	if !l.internalLock(ctx, id, res) {
		return false
	}

	defer l.internalUnLock(id, res)

	// check for the first call for this resource
	if _, ok := l.resources.Load(res); !ok {
		l.log.Debug("no such resource, creating new",
			zap.String("resource", res),
			zap.String("id", id),
			zap.Int("ttl microseconds", ttl),
		)

		r := &resource{
			mu:             sync.Mutex{},
			notificationCh: make(chan struct{}, 1),
			stopCh:         make(chan struct{}, 1),
			locks:          sync.Map{},
		}

		r.writerCount.Store(0)
		r.readerCount.Store(1)
		// save resource
		l.resources.Store(res, r)

		// we have TTL, create callback
		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		return true
	}

	rr, _ := l.resources.Load(res)
	r := rr.(*resource)

	// check tricky cases
	switch {
	// case when we have write lock
	case r.writerCount.Load() == 1:
		if r.readerCount.Load() > 0 {
			l.log.Error("write<->read lock incosistend state, w==1, r>0",
				zap.String("resource", res),
				zap.String("id", id))
			return false
		}
		// we have to wait here
		l.log.Debug("waiting to acquire a lock, w==1, r==0",
			zap.String("resource", res),
			zap.String("id", id))
		// allow to release mutexes
		select {
		case l.releaseMuCh <- struct{}{}:
			l.log.Debug("returning releaseMuCh mutex to temporarily allow releasing locks",
				zap.String("resource", res),
				zap.String("id", id))
		default:
			l.log.DPanic("releaseMuCh is full, skipping releaseMuCh updating",
				zap.String("resource", res),
				zap.String("id", id))
		}

		select {
		case <-r.notificationCh:
			// get release mutex back
			<-l.releaseMuCh
			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers",
					zap.String("resource", res),
					zap.String("id", id),
					zap.Uint64("writers", r.writerCount.Load()),
					zap.Uint64("readers", r.readerCount.Load()))
				return false
			}

			r.writerCount.Store(0)
			r.readerCount.Add(1)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.locks.Store(id, &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			return true
		case <-ctx.Done():
			l.log.Warn("failed to acquire a readlock, timeout exceeded, w==1, r==0",
				zap.String("resource", res),
				zap.String("id", id))
			return false
		}

		// case when we don't have writer and have 0 or more readers
	case r.writerCount.Load() == 0:
		l.log.Debug("adding read lock, w==0, r>=0",
			zap.String("resource", res),
			zap.String("id", id))
		// increase readers
		r.writerCount.Store(0)
		r.readerCount.Add(1)

		// we have TTL, create callback
		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		return true
	default:
		l.log.Error("unknown readlock state",
			zap.Uint64("writers", r.writerCount.Load()),
			zap.Uint64("readers", r.readerCount.Load()),
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}
}

func (l *locker) release(ctx context.Context, res, id string) bool {
	if !l.internalReleaseLock(ctx, id, res) {
		return false
	}
	defer l.internalReleaseUnLock(id, res)

	if _, ok := l.resources.Load(res); !ok {
		l.log.Warn("no such resource",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	rr, _ := l.resources.Load(res)
	r := rr.(*resource)

	rl, ok := r.locks.Load(id)
	if !ok {
		l.log.Warn("no such lock ID",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	// notify close by closing stopCh for the particular ID
	close(rl.(*item).stopCh)

	l.log.Debug("lock successfully released",
		zap.String("resource", res),
		zap.String("id", id))
	return true
}

func (l *locker) forceRelease(ctx context.Context, res string) bool {
	if !l.internalReleaseLock(ctx, "force-release", res) {
		return false
	}

	defer l.internalReleaseUnLock("force-release", res)

	l.log.Debug("force release mutex obtained", zap.String("resource", res))

	if _, ok := l.resources.Load(res); !ok {
		l.log.Warn("no such resource", zap.String("resource", res))
		return false
	}

	r, _ := l.resources.Load(res)
	rr := r.(*resource)
	// broadcast release signal

	rr.locks.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(*item)
		select {
		case v.stopCh <- struct{}{}:
			l.log.Debug("force release notification sent", zap.String("id", k))
		default:
		}
		return true
	})

	l.log.Debug("all force-release messages were sent", zap.String("resource", res))
	return true
}

func (l *locker) exists(ctx context.Context, res, id string) bool {
	if !l.internalLock(ctx, id, res) {
		return false
	}
	defer l.internalUnLock(id, res)

	rr, ok := l.resources.Load(res)
	if !ok {
		l.log.Warn("no such resource",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	r := rr.(*resource)

	// special case, check if we have any locks
	if id == "*" {
		atLeastOne := false
		r.locks.Range(func(_, _ any) bool {
			atLeastOne = true
			return false
		})
		return atLeastOne
	}

	if _, ok := r.locks.Load(id); !ok {
		l.log.Warn("no such lock ID",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	return true
}

func (l *locker) updateTTL(ctx context.Context, res, id string, ttl int) bool {
	if !l.internalLock(ctx, id, res) {
		return false
	}
	defer l.internalUnLock(id, res)

	l.log.Debug("updateTTL started",
		zap.String("resource", res),
		zap.String("id", id))

	rr, ok := l.resources.Load(res)

	if !ok {
		l.log.Warn("no such resource",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	rl, ok := rr.(*resource).locks.Load(id)
	if !ok {
		l.log.Warn("no such resource ID",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	select {
	case rl.(*item).updateTTLCh <- ttl:
		l.log.Debug("lock/rlocl TTL was successfully updated",
			zap.String("resource", res),
			zap.String("id", id))
	default:
		l.log.Error("failed to send notification about TTL update",
			zap.String("resource", res),
			zap.String("id", id))
	}

	return true
}

func (l *locker) makeLockCallback(res, id string, ttl int, r *resource) (callback, chan struct{}, chan int) {
	stopCbCh := make(chan struct{}, 1)
	updateTTLCh := make(chan int, 1)

	// at this point, when adding lock, we should not have the callback
	return func(lockID string, notifCh chan<- struct{}, sCh <-chan struct{}) {
		// case for the items without TTL. We should add such items to control their flow
		if ttl == 0 {
			ttl = 31555952000000 // year
		}
		// TTL channel
		ta := time.NewTicker(time.Microsecond * time.Duration(ttl))
	loop:
		select {
		case <-ta.C:
			l.log.Debug("r/lock: ttl expired",
				zap.String("resource", res),
				zap.String("id", lockID),
				zap.Int("ttl microseconds", ttl),
			)
			ta.Stop()
			// broadcast stop channel
		case <-sCh:
			l.log.Debug("r/lock: ttl removed, broadcast call",
				zap.String("resource", res),
				zap.String("id", lockID),
				zap.Int("ttl microseconds", ttl),
			)
			ta.Stop()
			// item stop channel
		case <-stopCbCh:
			l.log.Debug("r/lock: ttl removed, callback call",
				zap.String("resource", res),
				zap.String("id", lockID),
				zap.Int("ttl microseconds", ttl),
			)
			ta.Stop()
		case newTTL := <-updateTTLCh:
			l.log.Debug("updating r/lock ttl",
				zap.String("resource", res),
				zap.String("id", id),
				zap.Int("new_ttl_microsec", newTTL))
			ta.Reset(time.Microsecond * time.Duration(newTTL))
			// in case of TTL we don't need to remove the item, only update TTL
			goto loop
		}

		// we need to protect bunch of the atomic operations here per-resource
		r.mu.Lock()
		defer r.mu.Unlock()

		r.locks.Delete(id)

		if r.writerCount.Load() == 1 {
			r.writerCount.Store(0)
			r.readerCount.Store(0)
		}

		if r.readerCount.Load() > 0 {
			// reduce number of readers
			r.readerCount.Add(^uint64(0))
		}

		l.log.Debug("current writers and readers count",
			zap.Uint64("writers", r.writerCount.Load()),
			zap.Uint64("readers", r.readerCount.Load()),
			zap.String("resource", res),
			zap.String("deleted id", id))
		// we also have to check readers and writers to send notification
		if r.writerCount.Load() == 0 && r.readerCount.Load() == 0 {
			// only one resource, remove it
			// and send notification to the notification channel
			select {
			case notifCh <- struct{}{}:
				l.log.Debug("deleting the last lock, sending notification",
					zap.String("resource", res),
					zap.String("id", id))
			default:
				l.log.Debug("deleting the last lock, failed to send notification",
					zap.String("resource", res),
					zap.String("id", id))
				break
			}
		}
	}, stopCbCh, updateTTLCh
}

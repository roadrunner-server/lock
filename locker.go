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
	ownerID atomic.Pointer[string]
	// mutex is responsible for the locks, callback safety
	// lock is the exclusive lock (should be 1 or 0)
	writerCount atomic.Uint64
	// readerCount is the number of readers (writers must be 0)
	readerCount atomic.Uint64

	// lock with timeout based on channel
	resourceMu *reslock
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
	// global mutex for the resources, this is a short-lived lock to create or get resource
	globalMu *reslock
	// logger
	log *zap.Logger
	// all resources stored here
	resources map[string]*resource
}

func newLocker(log *zap.Logger) *locker {
	return &locker{
		log:       log,
		globalMu:  newResLock(),
		resources: make(map[string]*resource, 5),
	}
}

// lock used to acquire exclusive lock for the resource or promote read-lock to lock with the same ID
/*
Here may have the following scenarios:
- No resource associated with the resource ID, create resource, add callback if we have TTL, increate writers to 1.
- Resource exists, no locks associated with it -> add callback if we have TTL, increase writers to 1
- Resource exists, write lock already acquired. Wait on context and notification channel. Notification will be sent when the last lock is released.

*/
func (l *locker) lock(ctx context.Context, res, id string, ttl int) bool {
	// first - check if the resource exists
	// check if we have the resource
	obt := l.globalMu.lock(ctx)
	if !obt {
		l.log.Debug("failed to acquire a global lock",
			zap.String("resource", res),
			zap.String("id", id),
		)
		return false
	}

	r, ok := l.resources[res]
	if !ok {
		// write operation
		l.log.Debug("no such lock resource, creating new",
			zap.String("resource", res),
			zap.String("id", id),
			zap.Int("ttl microseconds", ttl),
		)

		rr := &resource{
			locks:          sync.Map{},
			resourceMu:     newResLock(),
			notificationCh: make(chan struct{}, 1),
			stopCh:         make(chan struct{}, 1),
		}

		rr.ownerID.Store(&id)
		rr.writerCount.Store(1)
		rr.readerCount.Store(0)

		l.resources[res] = rr

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		rr.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, rr.notificationCh, rr.stopCh)
		}()

		l.globalMu.unlock()
		return true
	}

	// after obtaining lock -> unlock the resource
	l.globalMu.unlock()

	// lock the resource
	if !r.resourceMu.lock(ctx) {
		l.log.Warn("can't acquire a lock for the resource, timeout exceeded",
			zap.String("res", res),
			zap.String("id", id),
		)

		return false
	}

	switch {
	// we have writer
	case r.readerCount.Load() == 0 && r.writerCount.Load() == 1:
		l.log.Debug("waiting to hold the mutex", zap.String("id", id))

		// unlock the release mutex
		r.resourceMu.unlockRelease()

		select {
		// wait for the notification
		case <-r.notificationCh:
			l.log.Debug("previous lock was removed, received notification", zap.String("id", id))
			// get release mutex back
			if !r.resourceMu.lockRelease(ctx) {
				l.log.Debug("can't acquire release mutex back, timeout exceeded",
					zap.String("resource", res),
					zap.String("id", id),
				)
				// unlock only the operation lock, since someone locked the release mutex
				r.resourceMu.unlockOperation()
				return false
			}

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

			r.ownerID.Store(&id)
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

			r.locks.Store(id, &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})
			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			r.resourceMu.unlock()
			return true
		case <-ctx.Done():
			l.log.Warn("lock notification wait timeout expired", zap.String("id", id))
			// at this moment we're holding only resource lock
			r.resourceMu.unlockOperation()
			return false
		}

		// we have readers
	case r.writerCount.Load() == 0 && r.readerCount.Load() > 0:
		// check if that's possible to elevate read lock permission to write
		if r.readerCount.Load() == 1 {
			l.log.Debug("checking readers to elevate rlock permissions, w==0, r>0", zap.String("id", id))

			rr, okk := r.locks.Load(id)
			if okk {
				// promote lock from read to write
				l.log.Debug("found read lock which can be promoted to write lock", zap.String("id", id))
				// send stop signal to the particular lock
				close(rr.(*item).stopCh)
			}

			r.resourceMu.unlockRelease()
			// wait for the notification
			l.log.Debug("waiting for the notification: r>0, w==0: r==0, w==0", zap.String("id", id))

			select {
			case <-r.notificationCh:
				l.log.Debug("r==0 notification received", zap.String("id", id))

				// get release mutex back
				if !r.resourceMu.lockRelease(ctx) {
					l.log.Debug("can't acquire release mutex back, timeout exceeded",
						zap.String("resource", res),
						zap.String("id", id),
					)
					// unlock only the operation lock, since someone locked the release mutex
					r.resourceMu.unlockOperation()
					return false
				}

				// inconsistent, still have readers/writers after notification
				if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
					l.log.Error("inconsistent state, should be zero writers and zero readers",
						zap.String("resource", res),
						zap.String("id", id),
						zap.Uint64("writers", r.writerCount.Load()),
						zap.Uint64("readers", r.readerCount.Load()))

					r.resourceMu.unlock()
					return false
				}

				// store writer and remove reader
				r.ownerID.Store(&id)
				r.writerCount.Store(1)
				r.readerCount.Store(0)

				// callback
				callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

				r.locks.Store(id, &item{
					callback:    callb,
					stopCh:      stopCbCh,
					updateTTLCh: updateTTLCh,
				})

				// run the callback
				go func() {
					callb(id, r.notificationCh, r.stopCh)
				}()

				r.resourceMu.unlock()
				return true
			case <-ctx.Done():
				l.log.Warn("lock notification wait timeout expired", zap.String("id", id))
				// at this moment we're holding only operation lock, release lock was sent back
				r.resourceMu.unlockOperation()
				return false
			}
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
			r.ownerID.Store(&id)
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			// callback
			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

			r.locks.Store(id, &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			r.resourceMu.unlock()
			return true
			// timeout exceeded
		case <-ctx.Done():
			l.log.Warn("timeout expired, no lock acquired",
				zap.String("resource", res),
				zap.String("id", id),
			)
			r.resourceMu.unlockOperation()
			return false
		}

		// noone holding any type of lock
	case r.readerCount.Load() == 0 && r.writerCount.Load() == 0:
		l.log.Debug("acquiring lock, w==0, r==0",
			zap.String("resource", res),
			zap.String("id", id))
		// drain notifications channel, just to be sure
		select {
		case <-r.notificationCh:
		default:
		}

		// store the writer
		r.ownerID.Store(&id)
		r.writerCount.Store(1)
		r.readerCount.Store(0)

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		r.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		r.resourceMu.unlock()
		return true
	default:
		l.log.Error("unknown readlock state",
			zap.String("resource", res),
			zap.String("id", id),
			zap.Uint64("writers", r.writerCount.Load()),
			zap.Uint64("readers", r.readerCount.Load()),
		)

		r.resourceMu.unlock()
		return false
	}
}

func (l *locker) lockRead(ctx context.Context, res, id string, ttl int) bool {
	// first - check if the resource exists
	// check if we have the resource
	obt := l.globalMu.lock(ctx)
	if !obt {
		l.log.Debug("failed to acquire a global lock",
			zap.String("resource", res),
			zap.String("id", id),
		)
		return false
	}

	r, ok := l.resources[res]
	if !ok {
		// write operation
		l.log.Debug("no such lock resource, creating new",
			zap.String("resource", res),
			zap.String("id", id),
			zap.Int("ttl microseconds", ttl),
		)

		rr := &resource{
			locks:          sync.Map{},
			resourceMu:     newResLock(),
			notificationCh: make(chan struct{}, 1),
			stopCh:         make(chan struct{}, 1),
		}

		ownerID := ""
		rr.ownerID.Store(&ownerID)
		rr.writerCount.Store(0)
		rr.readerCount.Store(1)

		l.resources[res] = rr

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		rr.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, rr.notificationCh, rr.stopCh)
		}()

		l.globalMu.unlock()
		return true
	}

	// after obtaining lock -> unlock the resource
	l.globalMu.unlock()

	// lock the resource
	if !r.resourceMu.lock(ctx) {
		l.log.Warn("can't acquire a lock for the resource, timeout exceeded",
			zap.String("res", res),
			zap.String("id", id),
		)

		return false
	}

	// check tricky cases
	switch {
	// case when we have write lock
	case r.writerCount.Load() == 1:
		if r.readerCount.Load() > 0 {
			l.log.Error("write<->read lock incosistend state, w==1, r>0",
				zap.String("resource", res),
				zap.String("id", id))

			r.resourceMu.unlock()
			return false
		}
		// we have to wait here
		l.log.Debug("waiting to acquire a lock, w==1, r==0",
			zap.String("resource", res),
			zap.String("id", id))
		// allow to release mutexes

		l.log.Debug("returning releaseMuCh mutex to temporarily allow releasing locks",
			zap.String("resource", res),
			zap.String("id", id))

		// unlock release mutex
		r.resourceMu.unlockRelease()

		select {
		case <-r.notificationCh:
			// get release mutex back
			if !r.resourceMu.lockRelease(ctx) {
				l.log.Debug("can't acquire release mutex back, timeout exceeded",
					zap.String("resource", res),
					zap.String("id", id),
				)
				// unlock only the operation lock, since someone locked the release mutex
				r.resourceMu.unlockOperation()
				return false
			}

			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers",
					zap.String("resource", res),
					zap.String("id", id),
					zap.Uint64("writers", r.writerCount.Load()),
					zap.Uint64("readers", r.readerCount.Load()))

				r.resourceMu.unlock()
				return false
			}

			r.writerCount.Store(0)
			r.readerCount.Add(1)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

			r.locks.Store(id, &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			r.resourceMu.unlock()
			return true
		case <-ctx.Done():
			l.log.Warn("failed to acquire a readlock, timeout exceeded, w==1, r==0",
				zap.String("resource", res),
				zap.String("id", id))

			r.resourceMu.unlockOperation()
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
		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		r.locks.Store(id, &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(id, r.notificationCh, r.stopCh)
		}()

		r.resourceMu.unlock()
		return true
	default:
		l.log.Error("unknown readlock state",
			zap.Uint64("writers", r.writerCount.Load()),
			zap.Uint64("readers", r.readerCount.Load()),
			zap.String("resource", res),
			zap.String("id", id))
		r.resourceMu.unlock()
		return false
	}
}

func (l *locker) release(ctx context.Context, res, id string) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			zap.String("resource", res),
			zap.String("id", id),
		)

		return false
	}
	// check if we have the resource
	r, ok := l.resources[res]
	if !ok {
		l.log.Debug("no such resource",
			zap.String("resource", res),
			zap.String("id", id),
		)

		l.globalMu.unlock()
		return false
	}

	l.globalMu.unlock()

	if !r.resourceMu.lockRelease(ctx) {
		l.log.Warn("failed to acquire a resource lock",
			zap.String("resource", res),
			zap.String("id", id),
		)
		return false
	}

	if r.ownerID.Load() != nil && *r.ownerID.Load() != "" && *r.ownerID.Load() != id {
		l.log.Debug("release called for the resource which is not owned by the caller",
			zap.String("resource", res),
			zap.String("id", id),
		)
		r.resourceMu.unlockRelease()
		return false
	}

	rl, ok := r.locks.Load(id)
	if !ok {
		l.log.Warn("no such lock ID",
			zap.String("resource", res),
			zap.String("id", id))

		r.resourceMu.unlockRelease()
		return false
	}

	// notify close by closing stopCh for the particular ID
	close(rl.(*item).stopCh)

	l.log.Debug("lock successfully released",
		zap.String("resource", res),
		zap.String("id", id))

	r.resourceMu.unlockRelease()
	return true
}

func (l *locker) forceRelease(ctx context.Context, res string) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			zap.String("resource", res),
		)
		return false
	}
	// check if we have the resource
	r, ok := l.resources[res]
	if !ok {
		l.log.Debug("no such resource", zap.String("resource", res))

		l.globalMu.unlock()
		return false
	}

	l.globalMu.unlock()

	if !r.resourceMu.lockRelease(ctx) {
		l.log.Warn("failed to acquire a resource lock",
			zap.String("resource", res),
		)
		return false
	}
	// broadcast release signal

	r.locks.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(*item)
		select {
		case v.stopCh <- struct{}{}:
			l.log.Debug("force release notification sent", zap.String("id", k))
		default:
		}
		return true
	})

	r.resourceMu.unlockRelease()
	l.log.Debug("all force-release messages were sent", zap.String("resource", res))
	return true
}

func (l *locker) exists(ctx context.Context, res, id string) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			zap.String("resource", res),
			zap.String("id", id),
		)
		return false
	}

	defer l.globalMu.unlock()

	r, ok := l.resources[res]
	if !ok {
		l.log.Warn("no such resource",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	// special case, check if we have any locks
	if id == "*" {
		if r.writerCount.Load() > 0 || r.readerCount.Load() > 0 {
			return true
		}

		return false
	}

	if _, existsID := r.locks.Load(id); !existsID {
		l.log.Warn("no such lock ID",
			zap.String("resource", res),
			zap.String("id", id))
		return false
	}

	return true
}

func (l *locker) updateTTL(ctx context.Context, res, id string, ttl int) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			zap.String("resource", res),
			zap.String("id", id),
		)
		return false
	}

	r, ok := l.resources[res]
	if !ok {
		l.log.Warn("no such resource",
			zap.String("resource", res),
			zap.String("id", id))

		l.globalMu.unlock()
		return false
	}

	l.globalMu.unlock()

	// lock the resource
	if !r.resourceMu.lockRelease(ctx) {
		l.log.Warn("can't acquire a lock for the resource, timeout exceeded",
			zap.String("res", res),
			zap.String("id", id),
		)

		return false
	}

	l.log.Debug("updateTTL started",
		zap.String("resource", res),
		zap.String("id", id))

	if !ok {
		l.log.Warn("no such resource",
			zap.String("resource", res),
			zap.String("id", id))

		r.resourceMu.unlockRelease()
		return false
	}

	rl, ok := r.locks.Load(id)
	if !ok {
		l.log.Warn("no such resource ID",
			zap.String("resource", res),
			zap.String("id", id))
		r.resourceMu.unlockRelease()
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

	r.resourceMu.unlockRelease()
	return true
}

func (l *locker) stop(ctx context.Context) {
	l.log.Debug("received stop signal, acquiring lock/release mutexes")
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release")
		return
	}

	defer l.globalMu.unlock()

	l.log.Debug("acquired stop mutex")

	for k, v := range l.resources {
		close(v.stopCh)
		l.log.Debug("closed broadcast channed",
			zap.String("resource", ""),
			zap.String("id", k))
	}

	l.log.Debug("signal sent to all resources")
}

func (l *locker) makeLockCallback(res, id string, ttl int) (callback, chan struct{}, chan int) {
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

		if !l.globalMu.lock(context.Background()) {
			l.log.Debug("failed to acquire a global lock for release",
				zap.String("resource", res),
				zap.String("id", id),
			)
			return
		}
		defer l.globalMu.unlock()

		// remove the item
		// we need to protect bunch of the atomic operations here per-resource
		r, ok := l.resources[res]
		if !ok {
			l.log.Warn("no such resource, TTL expired",
				zap.String("resource", res),
				zap.String("id", id))
			return
		}

		r.locks.Delete(id)

		if r.writerCount.Load() == 1 {
			// clear owner, only writer might be an owner
			ownerID := ""
			r.ownerID.Store(&ownerID)
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
				l.log.Debug("deleting the last lock for the resource, sending notification",
					zap.String("resource", res),
					zap.String("id", id))
			default:
				l.log.Debug("deleting the last lock for the resource, failed to send notification",
					zap.String("resource", res),
					zap.String("id", id))
				break
			}
		}
	}, stopCbCh, updateTTLCh
}

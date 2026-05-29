package lock

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// callback function is a function that should be executed right after it inserted into hashmap
// generally callback is responsible for the removing itself from the hashmap
// notifyCh - channel to notify that all locks were removed
// stopCh - broadcast channel to stop all the callbacks associated with the resource
type callback func(notifyCh chan<- struct{}, stopCh <-chan struct{})

// item represents callback element
type item struct {
	// item's stop channel
	stopCh chan struct{}
	// item's update TTL channel
	updateTTLCh chan int
}

// resource
type resource struct {
	ownerID atomic.Pointer[string]
	// writerCount is the exclusive lock counter (should be 1 or 0)
	writerCount atomic.Uint64
	// readerCount is the number of readers (writers must be 0)
	readerCount atomic.Uint64

	// lock with timeout based on a channel
	resourceMu *reslock
	// map with the actual locks by ID
	locks sync.Map // map[string]*item

	// notificationCh used to notify that all locks are expired and the user is free to get a new one
	// this channel receives an event only if there are no locks (write/read)
	//  resource-based
	notificationCh chan struct{}
	// stopCh should not receive any events. It's used as a broadcast-on-close event to notify all existing locks to expire
	stopCh chan struct{}
}

type locker struct {
	// global mutex for the resources, this is a short-lived lock to create or get resource
	globalMu *reslock
	// logger
	log *slog.Logger
	// all resources stored here
	resources map[string]*resource
}

func newLocker(log *slog.Logger) *locker {
	return &locker{
		log:       log,
		globalMu:  newResLock(log),
		resources: make(map[string]*resource, 5),
	}
}

// lock used to acquire exclusive lock for the resource or promote read-lock to lock with the same ID
/*
Here may have the following scenarios:
- No resource associated with the resource ID, create resource, add callback if we have TTL, increase writers to 1.
- Resource exists, no locks associated with it -> add callback if we have TTL, increase writers to 1
- Resource exists, write lock already acquired.
Wait on a context and notification channel.
Notification will be sent when the last lock is released.

*/
func (l *locker) lock(ctx context.Context, res, id string, ttl int) bool {
	// first - check if the resource exists,
	// check if we have the resource
	obt := l.globalMu.lock(ctx)
	if !obt {
		l.log.Debug("failed to acquire a global lock",
			"resource", res,
			"id", id,
		)
		return false
	}

	r, ok := l.resources[res]
	if !ok {
		// write operation
		l.log.Debug("no such lock resource, creating new",
			"resource", res,
			"id", id,
			"ttl microseconds", ttl,
		)

		rr := &resource{
			resourceMu:     newResLock(l.log),
			notificationCh: make(chan struct{}, 1),
			stopCh:         make(chan struct{}, 1),
		}

		rr.ownerID.Store(new(id))
		rr.writerCount.Store(1)

		l.resources[res] = rr

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		rr.locks.Store(id, &item{
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(rr.notificationCh, rr.stopCh)
		}()

		l.globalMu.unlock()
		return true
	}

	// after getting lock -> unlock the resource
	l.globalMu.unlock()

	// lock the resource
	if !r.resourceMu.lock(ctx) {
		l.log.Warn("can't acquire a lock for the resource, timeout exceeded",
			"resource", res,
			"id", id,
		)

		return false
	}

	switch {
	// we have a writer
	case r.readerCount.Load() == 0 && r.writerCount.Load() == 1:
		l.log.Debug("waiting to hold the mutex", "id", id)

		// unlock the release mutex
		r.resourceMu.unlockRelease()

		l.log.Debug("release mutex unlocked",
			"resource", res,
			"id", id,
		)

		select {
		// wait for the notification
		case <-r.notificationCh:
			l.log.Debug("previous lock was removed, received notification", "id", id)
			// get release mutex back
			if !r.resourceMu.lockRelease(ctx) {
				l.log.Debug("can't acquire release mutex back, timeout exceeded",
					"resource", res,
					"id", id,
				)
				// unlock only the operation lock, since someone locked the release mutex
				r.resourceMu.unlockOperation()
				return false
			}

			l.log.Debug("got release mutex back", "id", id)

			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 || r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers",
					"resource", res,
					"id", id,
					"writers", r.writerCount.Load(),
					"readers", r.readerCount.Load())
				return false
			}

			r.ownerID.Store(new(id))
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

			r.locks.Store(id, &item{
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})
			// run the callback
			go func() {
				callb(r.notificationCh, r.stopCh)
			}()

			r.resourceMu.unlock()

			l.log.Debug("lock successfully acquired",
				"resource", res,
				"id", id,
			)
			return true
		case <-ctx.Done():
			l.log.Warn("lock notification wait timeout expired",
				"resource", res,
				"id", id,
			)
			// at this moment we're holding only the resource lock
			r.resourceMu.unlockOperation()
			return false
		}

		// we have readers
	case r.writerCount.Load() == 0 && r.readerCount.Load() > 0:
		// check if that's possible to elevate read lock permission to write
		if r.readerCount.Load() == 1 {
			l.log.Debug("checking readers to elevate rlock permissions, w==0, r>0",
				"resource", res,
				"id", id,
			)

			rr, okk := r.locks.Load(id)
			if okk {
				// promote lock from read to write
				l.log.Debug("found read lock which can be promoted to write lock",
					"resource", res,
					"id", id,
				)

				// send stop signal to the particular lock, here we have only 1 lock, we can send stop signal
				// instead of closing the channel
				select {
				case rr.(*item).stopCh <- struct{}{}:
				default:
					l.log.Debug("failed to send stop signal to the lock id, channel is full",
						"resource", res,
						"id", id,
					)
				}
			}

			r.resourceMu.unlockRelease()
			// wait for the notification
			l.log.Debug("waiting for the notification: r>0, w==0: r==0, w==0",
				"resource", res,
				"id", id,
			)

			select {
			case <-r.notificationCh:
				l.log.Debug("r==0 notification received",
					"resource", res,
					"id", id,
				)

				// get release mutex back
				if !r.resourceMu.lockRelease(ctx) {
					l.log.Debug("can't acquire release mutex back, timeout exceeded",
						"resource", res,
						"id", id,
					)
					// unlock only the operation lock, since someone locked the release mutex
					r.resourceMu.unlockOperation()
					return false
				}

				// inconsistent, still have readers/writers after notification
				if r.writerCount.Load() != 0 || r.readerCount.Load() != 0 {
					l.log.Error("inconsistent state, should be zero writers and zero readers",
						"resource", res,
						"id", id,
						"writers", r.writerCount.Load(),
						"readers", r.readerCount.Load())

					r.resourceMu.unlock()
					return false
				}

				// store writer and remove reader
				r.ownerID.Store(new(id))
				r.writerCount.Store(1)
				r.readerCount.Store(0)

				// callback
				callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

				r.locks.Store(id, &item{
					stopCh:      stopCbCh,
					updateTTLCh: updateTTLCh,
				})

				// run the callback
				go func() {
					callb(r.notificationCh, r.stopCh)
				}()

				r.resourceMu.unlock()
				return true
			case <-ctx.Done():
				l.log.Warn("lock notification wait timeout expired", "id", id)
				// at this moment we're holding only operation lock, release lock was sent back
				r.resourceMu.unlockOperation()
				return false
			}
		}

		// at this point we know that we have more than 1 read lock, so we can't promote them to lock
		l.log.Debug("waiting for the readlocks to expire/release, w==0, r>0",
			"resource", res,
			"id", id)

		select {
		// we've got notification that no one holding this mutex anymore
		case <-r.notificationCh:
			l.log.Debug("no readers holding mutex anymore, proceeding with acquiring write lock", "id", id)
			// store writer and remove reader
			r.ownerID.Store(new(id))
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			// callback
			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

			r.locks.Store(id, &item{
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})

			// run the callback
			go func() {
				callb(r.notificationCh, r.stopCh)
			}()

			r.resourceMu.unlock()
			return true
			// timeout exceeded
		case <-ctx.Done():
			l.log.Warn("timeout expired, no lock acquired",
				"resource", res,
				"id", id,
			)
			r.resourceMu.unlockOperation()
			return false
		}

		// noone holding any type of lock
	case r.readerCount.Load() == 0 && r.writerCount.Load() == 0:
		l.log.Debug("acquiring lock, w==0, r==0",
			"resource", res,
			"id", id)
		// drain notifications channel, just to be sure
		select {
		case <-r.notificationCh:
		default:
		}

		// store the writer
		r.ownerID.Store(new(id))
		r.writerCount.Store(1)
		r.readerCount.Store(0)

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		r.locks.Store(id, &item{
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(r.notificationCh, r.stopCh)
		}()

		r.resourceMu.unlock()
		return true
	default:
		l.log.Error("unknown readlock state",
			"resource", res,
			"id", id,
			"writers", r.writerCount.Load(),
			"readers", r.readerCount.Load(),
		)

		r.resourceMu.unlock()
		return false
	}
}

func (l *locker) lockRead(ctx context.Context, res, id string, ttl int) bool {
	// first - check if the resource exists,
	// check if we have the resource
	obt := l.globalMu.lock(ctx)
	if !obt {
		l.log.Debug("failed to acquire a global lock",
			"resource", res,
			"id", id,
		)
		return false
	}

	r, ok := l.resources[res]
	if !ok {
		// write operation
		l.log.Debug("no such lock resource, creating new",
			"resource", res,
			"id", id,
			"ttl microseconds", ttl,
		)

		rr := &resource{
			resourceMu:     newResLock(l.log),
			notificationCh: make(chan struct{}, 1),
			stopCh:         make(chan struct{}, 1),
		}

		rr.ownerID.Store(new(""))
		rr.readerCount.Store(1)

		l.resources[res] = rr

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		rr.locks.Store(id, &item{
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(rr.notificationCh, rr.stopCh)
		}()

		l.globalMu.unlock()
		return true
	}

	// after getting lock -> unlock the resource
	l.globalMu.unlock()

	// lock the resource
	if !r.resourceMu.lock(ctx) {
		l.log.Warn("can't acquire a lock for the resource, timeout exceeded",
			"res", res,
			"id", id,
		)

		return false
	}

	// check tricky cases
	switch {
	// case when we have written lock
	case r.writerCount.Load() == 1:
		if r.readerCount.Load() > 0 {
			l.log.Error("write<->read lock incosistend state, w==1, r>0",
				"resource", res,
				"id", id)

			r.resourceMu.unlock()
			return false
		}
		// we have to wait here
		l.log.Debug("waiting to acquire a lock, w==1, r==0",
			"resource", res,
			"id", id)
		// allow releasing mutexes

		l.log.Debug("returning releaseMuCh mutex to temporarily allow releasing locks",
			"resource", res,
			"id", id)

		// unlock release mutex
		r.resourceMu.unlockRelease()

		select {
		case <-r.notificationCh:
			// get release mutex back
			if !r.resourceMu.lockRelease(ctx) {
				l.log.Debug("can't acquire release mutex back, timeout exceeded",
					"resource", res,
					"id", id,
				)
				// unlock only the operation lock, since someone locked the release mutex
				r.resourceMu.unlockOperation()
				return false
			}

			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 || r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers",
					"resource", res,
					"id", id,
					"writers", r.writerCount.Load(),
					"readers", r.readerCount.Load())

				r.resourceMu.unlock()
				return false
			}

			r.writerCount.Store(0)
			r.readerCount.Add(1)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

			r.locks.Store(id, &item{
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			})

			// run the callback
			go func() {
				callb(r.notificationCh, r.stopCh)
			}()

			r.resourceMu.unlock()
			return true
		case <-ctx.Done():
			l.log.Warn("failed to acquire a readlock, timeout exceeded, w==1, r==0",
				"resource", res,
				"id", id)

			r.resourceMu.unlockOperation()
			return false
		}

		// case when we don't have a writer and have 0 or more readers
	case r.writerCount.Load() == 0:
		l.log.Debug("adding read lock, w==0, r>=0",
			"resource", res,
			"id", id)
		// increase readers
		r.readerCount.Add(1)

		// we have TTL, create callback
		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl)

		r.locks.Store(id, &item{
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
		})

		// run the callback
		go func() {
			callb(r.notificationCh, r.stopCh)
		}()

		r.resourceMu.unlock()
		return true
	default:
		l.log.Error("unknown readlock state",
			"writers", r.writerCount.Load(),
			"readers", r.readerCount.Load(),
			"resource", res,
			"id", id)
		r.resourceMu.unlock()
		return false
	}
}

func (l *locker) release(ctx context.Context, res, id string) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			"resource", res,
			"id", id,
		)

		return false
	}
	// check if we have the resource
	r, ok := l.resources[res]
	if !ok {
		l.log.Debug("no such resource",
			"resource", res,
			"id", id,
		)

		l.globalMu.unlock()
		return false
	}

	l.globalMu.unlock()

	if !r.resourceMu.lockRelease(ctx) {
		l.log.Warn("failed to acquire a resource lock",
			"resource", res,
			"id", id,
		)
		return false
	}

	if r.ownerID.Load() != nil && *r.ownerID.Load() != "" && *r.ownerID.Load() != id {
		l.log.Debug("release called for the resource which is not owned by the caller",
			"resource", res,
			"id", id,
		)
		r.resourceMu.unlockRelease()
		return false
	}

	rl, ok := r.locks.Load(id)
	if !ok {
		l.log.Warn("no such lock ID",
			"resource", res,
			"id", id)

		r.resourceMu.unlockRelease()
		return false
	}

	// notify close by closing stopCh for the particular ID
	select {
	case rl.(*item).stopCh <- struct{}{}:
		l.log.Debug("force release notification sent",
			"resource", res,
			"id", id,
		)
	default:
	}

	l.log.Debug("lock successfully released",
		"resource", res,
		"id", id)

	r.resourceMu.unlockRelease()
	return true
}

func (l *locker) forceRelease(ctx context.Context, res string) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			"resource", res,
		)
		return false
	}
	// check if we have the resource
	r, ok := l.resources[res]
	if !ok {
		l.log.Debug("no such resource", "resource", res)

		l.globalMu.unlock()
		return false
	}

	l.globalMu.unlock()

	if !r.resourceMu.lockRelease(ctx) {
		l.log.Warn("failed to acquire a resource lock",
			"resource", res,
		)
		return false
	}

	// broadcast release signal
	r.locks.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(*item)
		select {
		case v.stopCh <- struct{}{}:
			l.log.Debug("force release notification sent", "id", k)
		default:
		}
		return true
	})

	r.resourceMu.unlockRelease()
	l.log.Debug("all force-release messages were sent", "resource", res)
	return true
}

func (l *locker) exists(ctx context.Context, res, id string) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			"resource", res,
			"id", id,
		)
		return false
	}

	defer l.globalMu.unlock()

	r, ok := l.resources[res]
	if !ok {
		l.log.Warn("no such resource",
			"resource", res,
			"id", id)
		return false
	}

	// Special case, check if we have any locks
	if id == "*" {
		return r.writerCount.Load() > 0 || r.readerCount.Load() > 0
	}

	if _, existsID := r.locks.Load(id); !existsID {
		l.log.Warn("no such lock ID",
			"resource", res,
			"id", id)
		return false
	}

	return true
}

func (l *locker) updateTTL(ctx context.Context, res, id string, ttl int) bool {
	// write operation
	if !l.globalMu.lock(ctx) {
		l.log.Debug("failed to acquire a global lock for release",
			"resource", res,
			"id", id,
		)
		return false
	}

	r, ok := l.resources[res]
	if !ok {
		l.log.Warn("no such resource",
			"resource", res,
			"id", id)

		l.globalMu.unlock()
		return false
	}

	l.globalMu.unlock()

	// lock the resource
	if !r.resourceMu.lockRelease(ctx) {
		l.log.Warn("can't acquire a lock for the resource, timeout exceeded",
			"res", res,
			"id", id,
		)

		return false
	}

	l.log.Debug("updateTTL started",
		"resource", res,
		"id", id)

	rl, ok := r.locks.Load(id)
	if !ok {
		l.log.Warn("no such resource ID",
			"resource", res,
			"id", id)
		r.resourceMu.unlockRelease()
		return false
	}

	select {
	case rl.(*item).updateTTLCh <- ttl:
		l.log.Debug("lock/rlocl TTL was successfully updated",
			"resource", res,
			"id", id)
	default:
		l.log.Error("failed to send notification about TTL update",
			"resource", res,
			"id", id)
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
			"resource", "",
			"id", k)
	}

	l.log.Debug("signal sent to all resources")
}

func (l *locker) makeLockCallback(res, id string, ttl int) (callback, chan struct{}, chan int) {
	stopCbCh := make(chan struct{}, 1)
	updateTTLCh := make(chan int, 1)

	// at this point, when adding lock, we should not have the callback
	return func(notifCh chan<- struct{}, sCh <-chan struct{}) {
		// case for the items without TTL. We should add such items to control their flow
		cbttl := ttl
		if cbttl == 0 {
			cbttl = 31555952000000 // year
		}

		// TTL channel
		ta := time.NewTicker(time.Microsecond * time.Duration(cbttl))
		for {
			select {
			case <-ta.C:
				l.log.Debug("r/lock: ttl expired",
					"resource", res,
					"id", id,
					"ttl microseconds", cbttl,
				)
				ta.Stop()
				// broadcast stop channel
			case <-sCh:
				l.log.Debug("r/lock: ttl removed, stop broadcast call",
					"resource", res,
					"id", id,
					"ttl microseconds", cbttl,
				)
				ta.Stop()
				// item stop channel
			case <-stopCbCh:
				l.log.Debug("r/lock: ttl removed, stop callback call",
					"resource", res,
					"id", id,
					"ttl microseconds", cbttl,
				)
				ta.Stop()
			case newTTL := <-updateTTLCh:
				// if the new TTL is 0, we should treat it as unlimited
				if newTTL == 0 {
					newTTL = 31555952000000 // year
				}
				l.log.Debug("r/lock: ttl was updated",
					"resource", res,
					"id", id,
					"new ttl microseconds", newTTL)
				// update the initial ttl
				cbttl = newTTL
				ta.Reset(time.Microsecond * time.Duration(cbttl))
				// in case of TTL, we don't need to remove the item, only update TTL
				continue
			}
			break
		}

		// unlimited but should not be long
		if !l.globalMu.lock(context.Background()) {
			l.log.Debug("failed to acquire a global lock for release",
				"resource", res,
				"id", id,
			)
			return
		}
		defer l.globalMu.unlock()

		// remove the item
		// we need to protect a bunch of the atomic operations here per-resource
		r, ok := l.resources[res]
		if !ok {
			l.log.Warn("no such resource, TTL expired",
				"resource", res,
				"id", id)
			return
		}

		r.locks.Delete(id)

		if r.writerCount.Load() == 1 {
			// clear owner, only a writer might be an owner
			r.ownerID.Store(new(""))
			r.writerCount.Store(0)
			r.readerCount.Store(0)
		}

		if r.readerCount.Load() > 0 {
			// reduce the number of readers
			r.readerCount.Add(^uint64(0))
		}

		l.log.Debug("current writers and readers count",
			"resource", res,
			"deleted id", id,
			"writers", r.writerCount.Load(),
			"readers", r.readerCount.Load(),
		)

		// we also have to check readers and writers to send notification
		if r.writerCount.Load() == 0 && r.readerCount.Load() == 0 {
			// only one resource, remove it
			// and send a notification to the notification channel
			select {
			case notifCh <- struct{}{}:
				l.log.Debug("deleting the last lock for the resource, sending notification",
					"resource", res,
					"id", id)
			default:
				l.log.Debug("deleting the last lock for the resource, failed to send notification",
					"resource", res,
					"id", id)
				break
			}
		}
	}, stopCbCh, updateTTLCh
}

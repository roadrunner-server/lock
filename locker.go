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

type item struct {
	callback    callback
	stopCh      chan struct{}
	updateTTLCh chan int
}
type resourceC map[string]*item

// resource
type resource struct {
	// mutex is responsible for the locks, callback safety
	// mutex is allocated per-resource
	mu sync.Mutex
	// lock is the exclusive lock
	writerCount atomic.Uint64
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
		releaseMuCh: make(chan struct{}, 1),
		resources:   make(map[string]*resource, 5),
	}
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
	// lock with timeout
	// todo(rustatian): move to a function
	select {
	case <-l.muCh:
		// hold release mutex as well
		select {
		case <-l.releaseMuCh:
			l.log.Debug("acquiring lock and release mutexes")
		case <-ctx.Done():
			l.log.Warn("timeout exceeded, failed to acquire release lock")
			// we should return previously acquired lock mutex
			l.muCh <- struct{}{}
			return false
		}
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

		r.writerCount.Store(1)
		r.readerCount.Store(0)

		l.resources[res] = r

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.mu.Lock()
		r.locks[id] = &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
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

	// get resource
	// at this point both, lock and release lock should be acquired
	r := l.resources[res]

	// we don't have any lock associated with this resource
	switch {
	// we have writer
	case r.readerCount.Load() == 0 && r.writerCount.Load() == 1:
		l.log.Debug("waiting to hold the mutex")

		// here we allowing to release mutexes, because some of them might not have TTL and should be released manually
		l.releaseMuCh <- struct{}{}
		select {
		case <-r.notificationCh:
			l.log.Debug("receive notification")
			// get release mutex back
			<-l.releaseMuCh

			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers", zap.Uint64("writers", r.writerCount.Load()), zap.Uint64("readers", r.readerCount.Load()))
				return false
			}

			r.writerCount.Store(1)
			r.readerCount.Store(0)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.mu.Lock()
			r.locks[id] = &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
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
			l.log.Debug("wait timeout expired")
			return false
		}

	case r.readerCount.Load() > 0 && r.writerCount.Load() == 0:
		// check if that's possible to elevate read lock permission to write
		if r.readerCount.Load() == 1 {
			l.log.Debug("checking readers to elevate rlock permissions")
			r.mu.Lock()

			// check
			if _, ok := r.locks[id]; ok {
				// promote lock from read to write
				l.log.Debug("found read lock which can be promoted to write lock", zap.String("id", id))
				// send stop signal to the particular lock
				close(r.locks[id].stopCh)
			}
			r.mu.Unlock()
			// wait for the notification
			<-r.notificationCh

			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers", zap.Uint64("writers", r.writerCount.Load()), zap.Uint64("readers", r.readerCount.Load()))
				return false
			}

			// store writer and remove reader
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			// callback
			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.mu.Lock()
			r.locks[id] = &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			}
			r.mu.Unlock()

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			// return mutexes
			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}

			return true
		}

		// at this point we know, that we have more than 1 read lock, so, we can't promote them to lock
		l.log.Debug("waiting for the readlocks to expire/release", zap.String("resource", res))
		select {
		// we've got notification, that noone holding this mutex anymore
		case <-r.notificationCh:
			l.log.Debug("no readers holding mutex anymore, proceeding with acquiring write lock")
			// store writer and remove reader
			r.writerCount.Store(1)
			r.readerCount.Store(0)

			// callback
			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.mu.Lock()
			r.locks[id] = &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
			}
			r.mu.Unlock()

			// run the callback
			go func() {
				callb(id, r.notificationCh, r.stopCh)
			}()

			// return mutexes
			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}

			return true
			// timeout exceeded
		case <-ctx.Done():
			l.log.Warn("timeout expired, no lock acquired", zap.String("id", id), zap.String("resource", res))
			return false
		}

	case r.readerCount.Load() == 0 && r.writerCount.Load() == 0:
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

			// release mutex
			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return false
		}
		r.mu.Unlock()

		// store the writer
		r.writerCount.Store(1)
		r.readerCount.Store(0)

		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.mu.Lock()
		r.locks[id] = &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
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
	// only 1 goroutine might be passed here at the time
	// lock with timeout
	// todo(rustatian): move to a function
	select {
	case <-l.muCh:
		// hold release mutex as well
		select {
		case <-l.releaseMuCh:
			l.log.Debug("acquiring rlock and release mutexes")
		case <-ctx.Done():
			l.log.Warn("timeout exceeded, failed to acquire release lock")
			// we should return previously acquired lock mutex
			l.muCh <- struct{}{}
			return false
		}
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a read lock")
		return false
	}

	// check for the first call for this resource
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

		r.writerCount.Store(0)
		r.readerCount.Store(1)
		// save resource
		l.resources[res] = r

		// user requested lock, check it by ID
		if _, ok := r.locks[id]; ok {
			l.log.Warn("id already exists", zap.String("id", id))

			l.muCh <- struct{}{}
			l.releaseMuCh <- struct{}{}
			return false
		}

		// we have TTL, create callback
		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.mu.Lock()
		r.locks[id] = &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
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

	// check tricky cases
	switch {
	// case when we have write lock
	case r.writerCount.Load() == 1 && r.readerCount.Load() == 0:
		// we have to wait here
		l.log.Debug("waiting to acquire the lock", zap.String("resource", res), zap.String("id", id))
		// allow to release mutexes
		l.releaseMuCh <- struct{}{}
		select {
		case <-r.notificationCh:
			// get release mutex back
			<-l.releaseMuCh
			// inconsistent, still have readers/writers after notification
			if r.writerCount.Load() != 0 && r.readerCount.Load() != 0 {
				l.log.Error("inconsistent state, should be zero writers and zero readers", zap.Uint64("writers", r.writerCount.Load()), zap.Uint64("readers", r.readerCount.Load()))
				return false
			}

			r.writerCount.Store(0)
			r.readerCount.Add(1)

			callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

			r.mu.Lock()
			r.locks[id] = &item{
				callback:    callb,
				stopCh:      stopCbCh,
				updateTTLCh: updateTTLCh,
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
			l.log.Warn("failed to acquire the readlock", zap.String("resource", res), zap.String("id", id))
			return false
		}

		// case when we don't have writer and have 0 or more readers
	case r.writerCount.Load() == 0:
		// increase readers
		r.writerCount.Store(0)
		r.readerCount.Add(1)

		// we have TTL, create callback
		callb, stopCbCh, updateTTLCh := l.makeLockCallback(res, id, ttl, r)

		r.mu.Lock()
		r.locks[id] = &item{
			callback:    callb,
			stopCh:      stopCbCh,
			updateTTLCh: updateTTLCh,
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
	// notify close by closing stopCh for the particular ID
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
	// broadcast release signal
	for _, v := range r.locks {
		select {
		case v.stopCh <- struct{}{}:
		default:
			continue
		}
	}
	// recreate stop channel
	r.mu.Unlock()

	select {
	case <-r.notificationCh:
		l.log.Debug("all locks dedicated to the resource are released", zap.String("resource", res))
		delete(l.resources, res)
		return true
	case <-ctx.Done():
		l.log.Error("critical error: failed to release locks for the resource", zap.String("resource", res))
		return false
	}
}

func (l *locker) exists(ctx context.Context, res, id string) bool {
	select {
	case <-l.muCh:
		// hold release mutex as well
		select {
		case <-l.releaseMuCh:
			l.log.Debug("acquiring rlock and release mutexes")
		case <-ctx.Done():
			l.log.Warn("timeout exceeded, failed to acquire release lock")
			// we should return previously acquired lock mutex
			l.muCh <- struct{}{}
			return false
		}
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire an exist operation lock")
		return false
	}

	if _, ok := l.resources[res]; !ok {
		l.log.Warn("no such resource", zap.String("resource", res), zap.String("id", id))
		return false
	}

	if _, ok := l.resources[res].locks[id]; !ok {
		l.log.Warn("no such resource ID", zap.String("resource", res), zap.String("id", id))
		return false
	}

	return true
}

func (l *locker) updateTTL(ctx context.Context, res, id string, ttl int) bool {
	select {
	case <-l.muCh:
		// hold release mutex as well
		select {
		case <-l.releaseMuCh:
			l.log.Debug("acquiring lock and release mutexes")
		case <-ctx.Done():
			l.log.Warn("timeout exceeded, failed to acquire release lock")
			// we should return previously acquired lock mutex
			l.muCh <- struct{}{}
			return false
		}
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire updateTTL lock")
		l.muCh <- struct{}{}
		l.releaseMuCh <- struct{}{}
		return false
	}

	if _, ok := l.resources[res]; !ok {
		l.log.Warn("no such resource", zap.String("resource", res), zap.String("id", id))
		l.muCh <- struct{}{}
		l.releaseMuCh <- struct{}{}
		return false
	}

	if _, ok := l.resources[res].locks[id]; !ok {
		l.log.Warn("no such resource ID", zap.String("resource", res), zap.String("id", id))
		l.muCh <- struct{}{}
		l.releaseMuCh <- struct{}{}
		return false
	}

	r := l.resources[res]
	r.mu.Lock()

	// send update with new TTL
	r.locks[id].updateTTLCh <- ttl
	r.mu.Unlock()
	l.log.Debug("lock/rlocl TTL was successfully updated")

	l.muCh <- struct{}{}
	l.releaseMuCh <- struct{}{}
	return true
}

func (l *locker) makeLockCallback(res, id string, ttl int, r *resource) (callback, chan struct{}, chan int) {
	stopCbCh := make(chan struct{}, 1)
	updateTTLCh := make(chan int, 1)

	// at this point, when adding lock, we should not have the callback
	return func(lockID string, notifCh chan<- struct{}, sCh <-chan struct{}) {
		// case for the items without TTL
		if ttl == 0 {
			ttl = 31555952000000 // year
		}
		// channel to forcibly remove the lock
		ta := time.NewTicker(time.Microsecond * time.Duration(ttl))
		// save the stop channel
	loop:
		select {
		case <-ta.C:
			l.log.Debug("lock: ttl expired",
				zap.String("id", lockID),
				zap.Int("ttl microseconds", ttl),
			)
			ta.Stop()
			// broadcast stop channel
		case <-sCh:
			l.log.Debug("lock: ttl removed, broadcast call",
				zap.String("id", lockID),
				zap.Int("ttl microseconds", ttl),
			)
			ta.Stop()
			// item stop channel
		case <-stopCbCh:
			l.log.Debug("lock: ttl removed, callback call",
				zap.String("id", lockID),
				zap.Int("ttl microseconds", ttl),
			)
			ta.Stop()
		case newTTL := <-updateTTLCh:
			l.log.Debug("updating lock ttl", zap.String("id", id), zap.String("res", res), zap.Int("new_ttl_microsec", newTTL))
			ta.Reset(time.Microsecond * time.Duration(newTTL))
			break loop
		}

		r.mu.Lock()
		delete(r.locks, id)
		// we also have to check readers and writers to send notification
		if r.writerCount.Load() == 1 || r.readerCount.Load() == 1 {
			// only one resource, remove it
			// and send notification to the notification channel
			l.log.Debug("deleting the last lock, sending notification", zap.String("id", id))
			l.log.Debug("current map data", zap.Any("data", r.locks))
			notifCh <- struct{}{}
		}

		if r.writerCount.Load() == 1 {
			r.writerCount.Store(0)
			r.readerCount.Store(0)
		}

		if r.readerCount.Load() > 0 {
			// reduce number of readers
			r.readerCount.Add(^uint64(0))
		}
		r.mu.Unlock()
	}, stopCbCh, updateTTLCh
}

func (l *locker) stop(ctx context.Context) {
	select {
	case <-l.muCh:
		// hold release mutex as well
		select {
		case <-l.releaseMuCh:
			l.log.Debug("acquiring lock and release mutexes")
		case <-ctx.Done():
			l.log.Warn("timeout exceeded, failed to acquire release lock")
			// we should return previously acquired lock mutex
			l.muCh <- struct{}{}
			return
		}
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire updateTTL lock")
		l.muCh <- struct{}{}
		l.releaseMuCh <- struct{}{}
		return
	}

	// release all mutexes
	for _, v := range l.resources {
		v.mu.Lock()
		for _, vv := range v.locks {
			select {
			case vv.stopCh <- struct{}{}:
			default:
				break
			}
		}
		v.mu.Unlock()
	}
	l.log.Debug("signal sent to all resources")
}

package lock

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/lock/v4/rlocks"
	"github.com/roadrunner-server/lock/v4/timer"
	"go.uber.org/zap"
)

// timers: https://blogtitle.github.io/go-advanced-concurrency-patterns-part-2-timers/
// resource
type res struct {
	// rlocks is the list of the read locks
	rlocks *rlocks.RLocks
	// lock is the exclusive lock
	lock atomic.Pointer[string]
}

type locker struct {
	log *zap.Logger

	mu   sync.Mutex
	cond sync.Cond

	timers *timer.Timers
	data   map[string]*res
}

func NewLocker(log *zap.Logger) *locker {
	return &locker{
		log:    log,
		data:   make(map[string]*res, 10),
		timers: timer.New(),
		cond: sync.Cond{
			L: &sync.Mutex{},
		},
	}
}

func (l *locker) lock(resource, id string, ttl, wait int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash
	if _, ok := l.data[resource]; !ok {
		l.log.Debug("no such resource, creating new",
			zap.String("id", id),
			zap.Int("ttl", ttl),
			zap.Int("wait", wait),
		)
		r := &res{
			rlocks: rlocks.NewRLocks(),
		}

		l.data[resource] = r

		r.lock.Store(ptrTo(id))

		if ttl == 0 {
			return true
		}

		l.timers.StoreAndWait(id, time.NewTimer(time.Second*time.Duration(ttl)), func() {
			// double-check the ID
			if *r.lock.Load() != id {
				return
			}
			// remove the lock
			r.lock.Store(nil)
			// send signal
			l.cond.Signal()
		})

		return true
	}

	// here we know, that the associated with the resource value exists
	r := l.data[resource]

	// easy case, if we don't have any lock
	if r.rlocks.Len() == 0 && r.lock.Load() == nil {
		r.lock.Store(ptrTo(id))

		if ttl == 0 {
			return true
		}

		l.timers.StoreAndWait(id, time.NewTimer(time.Second*time.Duration(ttl)), func() {
			// double-check the ID
			if *r.lock.Load() != id {
				return
			}
			// remove the lock
			r.lock.Store(nil)
			// send signal
			l.cond.Signal()
		})

		return true
	}

	// we want exclusive lock, but don't want to wait for it
	if (r.rlocks.Len() > 1 || r.lock.Load() != nil) && wait == 0 {
		// have a read locks
		return false
	}

	// we need to check if that the holder of the rlock is we
	if r.rlocks.Len() == 1 && r.lock.Load() == nil && wait == 0 {
		// update the read-lock to write-lock
		if r.rlocks.Remove(id) {
			// store the updated lock
			r.lock.Store(ptrTo(id))

			if ttl == 0 {
				return true
			}

			l.timers.StoreAndWait(id, time.NewTimer(time.Second*time.Duration(ttl)), func() {
				// double-check the ID
				if *r.lock.Load() != id {
					return
				}
				// remove the lock
				r.lock.Store(nil)
				// send signal
				l.cond.Signal()
			})

			return true
		}

		return false
	}

	// here is the case, where a lock(or readLock) acquired, and we want to wait for it
	if (r.rlocks.Len() >= 1 || r.lock.Load() != nil) && wait != 0 {
		tryMore := atomic.Bool{}
		tryMore.Store(true)

		stopTimerCh := make(chan struct{}, 1)

		go func() {
			waitT := time.NewTimer(time.Second * time.Duration(wait))
			for {
				select {
				case <-stopTimerCh:
					if !waitT.Stop() {
						waitT.Reset(time.Nanosecond)
						<-waitT.C
					}
					return
				case <-waitT.C:
					tryMore.Store(false)
					l.cond.Signal()
					return
				}
			}
		}()

		// if we have read-locks OR write locks
		l.cond.L.Lock()
		for r.rlocks.Len() > 0 || r.lock.Load() != nil {
			// pass 1 waiting goroutine for the release method
			l.cond.Wait()
			if tryMore.Load() {
				continue
			}

			l.cond.L.Unlock()
			return false
		}

		// stop the wait timer
		stopTimerCh <- struct{}{}

		if r.rlocks.Len() > 0 || r.lock.Load() != nil {
			panic("lock: mutual access which is protected by mutexes")
		}

		r.lock.Store(ptrTo(id))

		if ttl == 0 {
			l.cond.L.Unlock()
			return true
		}

		l.timers.StoreAndWait(id, time.NewTimer(time.Second*time.Duration(ttl)), func() {
			wl := r.lock.Load()
			// double-check the ID
			if *wl != id {
				return
			}
			// remove the lock
			r.lock.Store(nil)
			// signal the lock that we update the condition
			l.cond.Signal()
		})

		l.cond.L.Unlock()
		return true
	}

	return false
}

func (l *locker) lockRead(resource, id string, ttl, wait int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash
	if _, ok := l.data[resource]; !ok {
		r := &res{
			rlocks: rlocks.NewRLocks(),
		}

		r.rlocks.Append(id)
		l.data[resource] = r

		if ttl == 0 {
			return true
		}

		/*
			here we need to unlock the read lock, to do that, we're iterating over the all read locks and trying
			to find and delete our ID
		*/
		l.timers.StoreAndWait(id, time.NewTimer(time.Second*time.Duration(ttl)), func() {
			if r.rlocks.Remove(id) {
				// signal the goroutine on the Wait (if waiting)
				l.cond.Signal()
			}
		})

		return true
	}

	// here we know, that the associated with the resource value exists
	r := l.data[resource]

	// easy case, if we don't have any lock, wait doesn't matter here, since we can add the lock immediately
	if r.rlocks.Len() >= 0 && r.lock.Load() == nil {
		r.rlocks.Append(id)

		if ttl == 0 {
			return true
		}

		l.timers.StoreAndWait(id, time.NewTimer(time.Second*time.Duration(ttl)), func() {
			if r.rlocks.Remove(id) {
				// signal the goroutine on the Wait (if waiting)
				l.cond.Signal()
			}
		})

		return true
	}

	// write lock exists
	if r.lock.Load() != nil && wait == 0 {
		return false
	}

	if r.lock.Load() != nil && wait != 0 {
		tryMore := atomic.Bool{}
		tryMore.Store(true)

		stopTimerCh := make(chan struct{}, 1)

		go func() {
			waitT := time.NewTimer(time.Second * time.Duration(wait))
			for {
				select {
				case <-stopTimerCh:
					if !waitT.Stop() {
						waitT.Reset(time.Nanosecond)
						<-waitT.C
					}
					return
				case <-waitT.C:
					tryMore.Store(false)
					l.cond.Signal()
					return
				}
			}
		}()

		// if we have read-locks OR write locks
		l.cond.L.Lock()
		// we don't need to check the read locks here, since we can have any number of the active read locks
		for r.lock.Load() != nil {
			// pass 1 waiting goroutine for the release method
			l.cond.Wait()
			if tryMore.Load() {
				continue
			}

			l.cond.L.Unlock()
			return false
		}

		// stop the wait timer
		stopTimerCh <- struct{}{}

		if r.lock.Load() != nil {
			panic("lock_read: mutual access which is protected by mutexes")
		}
		r.rlocks.Append(id)

		if ttl == 0 {
			l.cond.L.Unlock()
			return true
		}

		l.timers.StoreAndWait(id, time.NewTimer(time.Second*time.Duration(ttl)), func() {
			if r.rlocks.Remove(id) {
				// signal the goroutine on the Wait (if waiting)
				l.cond.Signal()
			}
		})

		l.cond.L.Unlock()
		return true
	}

	return false
}

func (l *locker) release(resource, id string) bool {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	if _, ok := l.data[resource]; !ok {
		return false
	}

	r := l.data[resource]

	// we have a lock
	if r.lock.Load() != nil {
		lock := *r.lock.Load()
		// check if we may unlock the lock
		if lock == id {
			l.timers.Stop(id)
			r.lock.Store(nil)
			l.cond.Signal()
			return true
		}

		// can't unlock the lock with the different ID
		return false
	}

	// we have a read-lock
	if r.rlocks.Len() >= 1 {
		if r.rlocks.Remove(id) {
			// signal the goroutine on the Wait (if waiting)
			l.cond.Signal()
			return true
		}

		// no element with such ID
		return false
	}

	// ?? how to get here
	return false
}

func (l *locker) forceRelease(resource string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if r, ok := l.data[resource]; ok {
		if r.lock.Load() != nil {
			id := *r.lock.Load()
			l.timers.Stop(id)
			return true
		}

		if r.rlocks.Len() > 0 {
			ids := r.rlocks.RemoveAll()
			for i := 0; i < len(ids); i++ {
				l.timers.Stop(ids[i])
			}
		}
	}

	delete(l.data, resource)

	return true
}

func (l *locker) exists(resource, id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if r, ok := l.data[resource]; ok {
		if r.lock.Load() != nil && *r.lock.Load() == id {
			return true
		}

		if r.rlocks.Exists(id) {
			return true
		}
	}

	return false
}

func (l *locker) updateTTL(resource, id string, ttl int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.data[resource]; !ok {
		l.log.Debug("no such resource")
		return false
	}

	// do not allow 0 ttl
	if ttl == 0 {
		return false
	}

	r := l.data[resource]

	// update the TTL for the lock
	if (r.lock.Load() != nil && *r.lock.Load() == id) || (r.lock.Load() == nil && r.rlocks.Exists(id)) {
		l.log.Debug("updating the TTL",
			zap.String("resource", resource),
			zap.String("id", id),
			zap.Int("ttl", ttl),
		)
		l.timers.UpdateTTL(id, ttl)
		return true
	}

	l.log.Debug("no such id registered",
		zap.String("resource", resource),
		zap.String("id", id),
	)

	return false
}

func ptrTo[T any](val T) *T {
	return &val
}

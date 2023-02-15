package lock

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/lock/v4/rlocks"
	pq "github.com/roadrunner-server/sdk/v4/priority_queue"
	"go.uber.org/zap"
)

// timers: https://blogtitle.github.io/go-advanced-concurrency-patterns-part-2-timers/
// resource
type resource struct {
	// rlocks is the list of the read locks
	rlocks *rlocks.RLocks
	// lock is the exclusive lock
	lock atomic.Pointer[string]
}

type locker struct {
	log    *zap.Logger
	stopCh chan struct{}

	// cas variable
	// used in the CAS algorithm to check if we have mutex acquired
	locked *int64
	queue  *pq.BinHeap[item]
	mu     sync.Mutex //

	resources sync.Map //map[string]*res
}

func NewLocker(log *zap.Logger) *locker {
	l := &locker{
		queue:  pq.NewBinHeap[item](1000),
		log:    log,
		locked: ptrTo(int64(0)),
		stopCh: make(chan struct{}),
	}

	go func() {
		l.wait()
	}()

	return l
}

func (l *locker) wait() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			tn := time.Now().Unix()
			tmp := l.queue.PeekPriority()
			if tmp == -1 {
				continue
			}

			// check for the expired TTL
			if tn >= tmp {
				l.log.Debug("lock: expired, executing callback")
				// we have an item with the expired TTL, extract it and execute callback
				i := l.queue.ExtractMin()
				i.callback()
			}
		}
	}
}

func (l *locker) lock(res, id string, ttl int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	// only 1 goroutine might be passed here at the time

	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash
	if _, ok := l.resources.Load(res); !ok {
		l.log.Debug("no such resource, creating new",
			zap.String("id", id),
			zap.Int("ttl", ttl),
		)

		r := &resource{
			rlocks: rlocks.NewRLocks(),
		}

		// store the resource and lock
		l.resources.Store(res, r)
		r.lock.Store(ptrTo(id))

		if ttl == 0 {
			return true
		}

		l.queue.Insert(newItem(res, id, ttl, func() {
			// double-check the ID
			if *r.lock.Load() != id {
				l.log.Debug("inconsistent lock state",
					zap.String("original ID", id),
					zap.String("lock ID", *r.lock.Load()))
				return
			}

			l.log.Debug("lock: ttl expired",
				zap.String("id", id),
				zap.Int("ttl", ttl),
			)
			// remove the lock
			r.lock.Store(ptrTo(""))
		}))

		return true
	}

	// here we know, that the associated with the resource value exists
	rr, _ := l.resources.Load(res)
	r := rr.(*resource)

	// easy case, if we don't have any lock
	if r.rlocks.Len() == 0 && *r.lock.Load() == "" {
		r.lock.Store(ptrTo(id))

		if ttl == 0 {
			return true
		}

		l.queue.Insert(newItem(res, id, ttl, func() {
			// double-check the ID
			if *r.lock.Load() != id {
				l.log.Debug("inconsistent lock state",
					zap.String("original ID", id),
					zap.String("lock ID", *r.lock.Load()))
				return
			}

			l.log.Debug("lock: ttl expired",
				zap.String("id", id),
				zap.Int("ttl", ttl),
			)
			// remove the lock
			r.lock.Store(ptrTo(""))
		}))

		return true
	}

	// we want exclusive lock, but don't want to wait for it
	if r.rlocks.Len() >= 1 || *r.lock.Load() != "" {
		l.log.Debug("lock already obtained",
			zap.String("id", id),
			zap.Int("ttl", ttl),
		)
		// have a read locks
		return false
	}

	// we need to check if that the holder of the rlock is we
	if r.rlocks.Len() == 1 && *r.lock.Load() == "" {
		// update the read-lock to write-lock
		if r.rlocks.Remove(id) {
			// store the updated lock
			r.lock.Store(ptrTo(id))

			if ttl == 0 {
				return true
			}

			l.queue.Insert(newItem(res, id, ttl, func() {
				// double-check the ID
				if *r.lock.Load() != id {
					l.log.Debug("inconsistent lock state",
						zap.String("original ID", id),
						zap.String("lock ID", *r.lock.Load()))
					return
				}

				l.log.Debug("lock: ttl expired",
					zap.String("id", id),
					zap.Int("ttl", ttl),
				)
				// remove the lock
				r.lock.Store(ptrTo(""))
			}))

			return true
		}
	}

	return false
}

func (l *locker) lockRead(res, id string, ttl int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash
	if _, ok := l.resources.Load(res); !ok {
		l.log.Debug("no such resource, creating new",
			zap.String("id", id),
			zap.Int("ttl", ttl),
		)
		r := &resource{
			rlocks: rlocks.NewRLocks(),
		}

		r.lock.Store(ptrTo(""))
		r.rlocks.Append(id)
		l.resources.Store(res, r)

		if ttl == 0 {
			return true
		}

		/*
			here we need to unlock the read lock, to do that, we're iterating over the all read locks and trying
			to find and delete our ID
		*/
		l.queue.Insert(newItem(res, id, ttl, func() {
			l.log.Debug("read lock: ttl expired",
				zap.String("id", id),
				zap.Int("ttl", ttl),
			)
			r.rlocks.Remove(id)
		}))

		return true
	}

	// here we know, that the associated with the resource value exists
	rr, _ := l.resources.Load(res)
	r := rr.(*resource)

	// write lock exists
	if *r.lock.Load() != "" {
		return false
	}

	// easy case, if we don't have any lock, wait doesn't matter here, since we can add the lock immediately
	if r.rlocks.Len() >= 0 && *r.lock.Load() == "" {
		r.rlocks.Append(id)

		if ttl == 0 {
			return true
		}

		l.queue.Insert(newItem(res, id, ttl, func() {
			l.log.Debug("read lock: ttl expired",
				zap.String("id", id),
				zap.Int("ttl", ttl),
			)
			r.rlocks.Remove(id)
		}))

		return true
	}

	return false
}

func (l *locker) release(res, id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.resources.Load(res); !ok {
		l.log.Debug("no such resource", zap.String("id", id))
		return false
	}

	// here we know, that the associated with the resource value exists
	rr, _ := l.resources.Load(res)
	r := rr.(*resource)

	// we have a lock
	if *r.lock.Load() != "" {
		lock := *r.lock.Load()
		// check if we may unlock the lock
		if lock == id {
			l.log.Debug("lock released",
				zap.String("original ID", id),
				zap.String("lock ID", *r.lock.Load()))

			l.queue.Remove(id)

			r.lock.Store(ptrTo(""))
			return true
		}

		// can't unlock the lock with the different ID
		return false
	}

	// we have a read-lock
	if r.rlocks.Len() >= 1 {
		if r.rlocks.Remove(id) {
			l.log.Debug("read lock released", zap.String("ID", id))
			return true
		}
	}

	l.log.Debug("no lock registered with provided ID", zap.String("ID", id))
	return false
}

func (l *locker) forceRelease(res string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.resources.Load(res); !ok {
		l.log.Debug("no such resource", zap.String("res", res))
		return false
	}

	// here we know, that the associated with the resource value exists
	rr, _ := l.resources.LoadAndDelete(res)
	r := rr.(*resource)

	if *r.lock.Load() != "" {
		id := *r.lock.Load()
		l.queue.Remove(id)
		l.log.Debug("lock forcibly released", zap.String("lock ID", id))

		return true
	}

	if r.rlocks.Len() > 0 {
		ids := r.rlocks.RemoveAll()
		for i := 0; i < len(ids); i++ {
			l.queue.Remove(ids[i])
			l.log.Debug("read lock forcibly released", zap.String("read lock ID", ids[i]))
		}

		return true
	}

	return false
}

func (l *locker) exists(res, id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if rr, ok := l.resources.Load(res); ok {
		r := rr.(*resource)
		if *r.lock.Load() == id {
			return true
		}

		if r.rlocks.Exists(id) {
			return true
		}
	}

	return false
}

func (l *locker) updateTTL(res, id string, ttl int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.resources.Load(res); !ok {
		l.log.Debug("no such resource")
		return false
	}

	// do not allow 0 ttl
	if ttl == 0 {
		return false
	}

	// here we know, that the associated with the resource value exists
	rr, _ := l.resources.Load(res)
	r := rr.(*resource)

	if *r.lock.Load() == "" {
		if r.rlocks.Exists(id) {
			vals := l.queue.Remove(id)
			// TTL expired
			if len(vals) == 0 {
				l.log.Debug("TTL already expired",
					zap.String("resource", res),
					zap.String("id", id),
					zap.Int("ttl", ttl),
				)
				return false
			}

			l.log.Debug("updating read-lock TTL",
				zap.String("resource", res),
				zap.String("id", id),
				zap.Int("ttl", ttl),
			)

			l.queue.Insert(newItem(res, id, ttl, func() {
				l.log.Debug("updated rlock: ttl expired",
					zap.String("id", id),
					zap.Int("ttl", ttl),
				)

				r.rlocks.Remove(id)
			}))
		} else {
			// no lock and read lock
			return false
		}
	}

	// we have lock
	vals := l.queue.Remove(id)
	// TTL expired
	if len(vals) == 0 {
		l.log.Debug("TTL already expired",
			zap.String("resource", res),
			zap.String("id", id),
			zap.Int("ttl", ttl),
		)
		return false
	}

	// update the TTL for the lock
	l.log.Debug("updating lock TTL",
		zap.String("resource", res),
		zap.String("id", id),
		zap.Int("ttl", ttl),
	)

	l.queue.Insert(newItem(res, id, ttl, func() {
		l.log.Debug("lock: ttl expired",
			zap.String("id", id),
			zap.Int("ttl", ttl),
		)
		r.lock.Store(ptrTo(""))
	}))

	return true
}

func (l *locker) stop() {
	l.stopCh <- struct{}{}
}

func ptrTo[T any](val T) *T {
	return &val
}

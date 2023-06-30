package lock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	mutexLocked = 1 << iota
	rMutexLocked
)

// callback function is a function which should be executed right after it inserted into hashmap
// generally callback is responsible of the removing itself from the hashmap
type callback func(resource, id string)
type resourceC map[string]callback

type item struct {
	callback       callback
	stopCh         chan struct{}
	notificationCh chan struct{}
}

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
	locks map[string]resourceC
}

type locker struct {
	// logger
	log *zap.Logger
	// stop all, close it to broadcast stop signal
	stopCh chan struct{}
	// mutex with tiemout based on channel
	muCh chan struct{}
	// all resources stored here
	resources map[string]*resource
}

func newLocker(log *zap.Logger) *locker {
	l := &locker{
		log:    log,
		muCh:   make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	l.muCh <- struct{}{}

	return l
}

func (l *locker) lock(ctx context.Context, res, id string, ttl int) bool {
	// lock with timeout
	// todo(rustatian): move to a function
	select {
	case <-l.muCh:
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.muCh <- struct{}{}
	}()

	// l.mu.Lock()
	// defer l.mu.Unlock()
	// only 1 goroutine might be passed here at the time

	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash

	if _, ok := l.resources[res]; !ok {
		l.log.Debug("no such resource, creating new",
			zap.String("id", id),
			zap.Int("ttl", ttl),
		)

		r := &resource{
			mu:    sync.Mutex{},
			locks: make(map[string]resourceC, 1),
		}
		r.writer.Store(1)
		r.readerCount.Store(0)

		if ttl == 0 {
			// no need to use callback if we don't have timeout
			return true
		}

		// user requested lock, check it by ID
		if _, ok := r.locks[id]; ok {
			l.log.Warn("id already exists", zap.String("id", id))
			return false
		}

		respondCh := make(chan struct{}, 1)
		stopCh := make(chan struct{}, 1)

		// at this point, when adding lock, we should not have the callback
		callb := func(resr, idl string) {
			// channel to forcibly remove the lock
			ta := time.After(time.Microsecond * time.Duration(ttl))
			// save the stop channel
			select {
			case <-ta:
				l.log.Debug("lock: ttl expired",
					zap.String("id", idl),
					zap.Int("ttl", ttl),
				)
			case <-stopCh:
				l.log.Debug("lock: ttl removed",
					zap.String("id", idl),
					zap.Int("ttl", ttl),
				)
			}

			r.mu.Lock()
			if len(r.locks[resr]) == 1 {
				// only one resource, remove it
				delete(r.locks, resr)
			} else {
				// delete id from the associated map
				// should not happen here, since this is lock handler
				delete(r.locks[resr], idl)
			}
			r.mu.Unlock()

			// remove the lock
			r.writer.Store(0)
			respondCh <- struct{}{}
		}

		r.mu.Lock()
		r.locks[res] = make(resourceC, 1)
		r.locks[res][id] = callb
		r.mu.Unlock()

		// run the callback
		go func() {
			callb(res, id)
		}()

		return true
	}

	// here we know, that the associated with the resource value exists
	// rr, _ := l.resources.Load(res)
	// r := rr.(*resource)
	//
	// // easy case, if we don't have any lock
	// if r.rlocks.Len() == 0 && *r.lock.Load() == "" {
	// 	r.lock.Store(ptrTo(id))
	//
	// 	if ttl == 0 {
	// 		return true
	// 	}
	//
	// 	respondCh := make(chan struct{}, 1)
	// 	// we have TTL, handle this case
	// 	l.queue[id] = newItem(res, id, ttl, func() {
	// 		// channel to forcibly remove the lock
	// 		stopCh := make(chan struct{}, 1)
	// 		ta := time.After(time.Microsecond * time.Duration(ttl))
	// 		// save the stop channel
	// 		l.ttls[id] = stopCh
	// 		select {
	// 		case <-ta:
	// 			l.log.Debug("lock: ttl expired",
	// 				zap.String("id", id),
	// 				zap.Int("ttl", ttl),
	// 			)
	// 		case <-stopCh:
	// 			l.log.Debug("lock: ttl removed",
	// 				zap.String("id", id),
	// 				zap.Int("ttl", ttl),
	// 			)
	// 		}
	// 		// clean the map
	// 		delete(l.ttls, id)
	// 		// send a feedback
	// 		respondCh <- struct{}{}
	// 		// remove the lock
	// 		r.lock.Store(ptrTo(""))
	// 	})
	//
	// 	return true
	// }
	//
	// // we want exclusive lock, but lock was already acquired
	// if *r.lock.Load() != "" {
	// 	if _, ok := l.queue[id]; !ok {
	// 		panic("fooo")
	// 	}
	//
	// 	resp := l.ttls[id]
	// 	if resp == nil {
	// 		panic("foo2")
	// 	}
	//
	// 	select {
	// 	case <-ctx.Done():
	// 		l.log.Warn("deadline exceeded, no lock acquired", zap.String("id", id))
	// 	case <-resp:
	// 		l.log.Debug("previous lock expired, ackquiring new lock", zap.String("id", id))
	// 		// double check queue
	// 		if _, ok := l.queue[id]; ok {
	// 			panic("impossible")
	// 		}
	//
	// 		r.lock.Store(ptrTo(id))
	//
	// 		if ttl == 0 {
	// 			l.log.Debug("lock succesfully acquired", zap.String("id", id))
	// 			return true
	// 		}
	//
	// 		respondCh := make(chan struct{}, 1)
	// 		// we have TTL, handle this case
	// 		l.queue[id] = newItem(res, id, ttl, func() {
	// 			// channel to forcibly remove the lock
	// 			stopCh := make(chan struct{}, 1)
	// 			ta := time.After(time.Microsecond * time.Duration(ttl))
	// 			// save the stop channel
	// 			l.ttls[id] = stopCh
	// 			select {
	// 			case <-ta:
	// 				l.log.Debug("lock: ttl expired",
	// 					zap.String("id", id),
	// 					zap.Int("ttl", ttl),
	// 				)
	// 			case <-stopCh:
	// 				l.log.Debug("lock: ttl removed",
	// 					zap.String("id", id),
	// 					zap.Int("ttl", ttl),
	// 				)
	// 			}
	// 			// clean the map
	// 			delete(l.ttls, id)
	// 			// send a feedback
	// 			respondCh <- struct{}{}
	// 			// remove the lock
	// 			r.lock.Store(ptrTo(""))
	// 		})
	// 	}
	// }
	//
	// if r.rlocks.Len() >= 1 || *r.lock.Load() != "" {
	// 	l.log.Debug("lock already obtained",
	// 		zap.String("id", id),
	// 		zap.Int("ttl", ttl),
	// 	)
	// 	// have a read locks
	// 	return false
	// }
	//
	// // we need to check if that the holder of the rlock is we
	// if r.rlocks.Len() == 1 && *r.lock.Load() == "" {
	// 	// update the read-lock to write-lock
	// 	if r.rlocks.Remove(id) {
	// 		// store the updated lock
	// 		r.lock.Store(ptrTo(id))
	//
	// 		if ttl == 0 {
	// 			return true
	// 		}
	//
	// 		respondCh := make(chan struct{}, 1)
	// 		// we have TTL, handle this case
	// 		l.queue[id] = newItem(res, id, ttl, func() {
	// 			// channel to forcibly remove the lock
	// 			stopCh := make(chan struct{}, 1)
	// 			ta := time.After(time.Microsecond * time.Duration(ttl))
	// 			// save the stop channel
	// 			l.ttls[id] = stopCh
	// 			select {
	// 			case <-ta:
	// 				l.log.Debug("lock: ttl expired",
	// 					zap.String("id", id),
	// 					zap.Int("ttl", ttl),
	// 				)
	// 			case <-stopCh:
	// 				l.log.Debug("lock: ttl removed",
	// 					zap.String("id", id),
	// 					zap.Int("ttl", ttl),
	// 				)
	// 			}
	// 			// clean the map
	// 			delete(l.ttls, id)
	// 			// send a feedback
	// 			respondCh <- struct{}{}
	// 			// remove the lock
	// 			r.lock.Store(ptrTo(""))
	// 		})
	//
	// 		return true
	// 	}
	// }

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

	// if there is no lock for the provided resource -> create it
	// assume that this is the first call to the lock with this resource and hash
	// if _, ok := l.resources.Load(res); !ok {
	// 	l.log.Debug("no such resource, creating new",
	// 		zap.String("id", id),
	// 		zap.Int("ttl", ttl),
	// 	)
	// 	r := &resource{
	// 		rlocks: rlocks.NewRLocks(),
	// 	}
	//
	// 	r.lock.Store(ptrTo(""))
	// 	r.rlocks.Append(id)
	// 	l.resources.Store(res, r)
	//
	// 	if ttl == 0 {
	// 		return true
	// 	}
	//
	// 	/*
	// 		here we need to unlock the read lock, to do that, we're iterating over the all read locks and trying
	// 		to find and delete our ID
	// 	*/
	// 	l.queue.Insert(newItem(res, id, ttl, func() {
	// 		l.log.Debug("read lock: ttl expired",
	// 			zap.String("id", id),
	// 			zap.Int("ttl", ttl),
	// 		)
	// 		r.rlocks.Remove(id)
	// 	}))
	//
	// 	return true
	// }
	//
	// // here we know, that the associated with the resource value exists
	// rr, _ := l.resources.Load(res)
	// r := rr.(*resource)
	//
	// // write lock exists
	// if *r.lock.Load() != "" {
	// 	return false
	// }
	//
	// // easy case, if we don't have any lock, wait doesn't matter here, since we can add the lock immediately
	// if r.rlocks.Len() >= 0 && *r.lock.Load() == "" {
	// 	r.rlocks.Append(id)
	//
	// 	if ttl == 0 {
	// 		return true
	// 	}
	//
	// 	l.queue.Insert(newItem(res, id, ttl, func() {
	// 		l.log.Debug("read lock: ttl expired",
	// 			zap.String("id", id),
	// 			zap.Int("ttl", ttl),
	// 		)
	// 		r.rlocks.Remove(id)
	// 	}))
	//
	// 	return true
	// }

	return false
}

func (l *locker) release(ctx context.Context, res, id string) bool {
	select {
	case <-l.muCh:
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.muCh <- struct{}{}
	}()

	// if _, ok := l.resources.Load(res); !ok {
	// 	l.log.Debug("no such resource", zap.String("id", id))
	// 	return false
	// }
	//
	// // here we know, that the associated with the resource value exists
	// rr, _ := l.resources.Load(res)
	// r := rr.(*resource)
	//
	// // we have a lock
	// if *r.lock.Load() != "" {
	// 	lock := *r.lock.Load()
	// 	// check if we may unlock the lock
	// 	if lock == id {
	// 		l.log.Debug("lock released",
	// 			zap.String("original ID", id),
	// 			zap.String("lock ID", *r.lock.Load()))
	//
	// 		l.queue.Remove(id)
	//
	// 		r.lock.Store(ptrTo(""))
	// 		return true
	// 	}
	//
	// 	// can't unlock the lock with the different ID
	// 	return false
	// }
	//
	// // we have a read-lock
	// if r.rlocks.Len() >= 1 {
	// 	if r.rlocks.Remove(id) {
	// 		l.log.Debug("read lock released", zap.String("ID", id))
	// 		return true
	// 	}
	// }

	l.log.Debug("no lock registered with provided ID", zap.String("ID", id))
	return false
}

func (l *locker) forceRelease(ctx context.Context, res string) bool {
	select {
	case <-l.muCh:
	case <-ctx.Done():
		l.log.Warn("timeout exceeded, failed to acquire a lock")
		return false
	}
	defer func() {
		l.muCh <- struct{}{}
	}()

	// if _, ok := l.resources.Load(res); !ok {
	// 	l.log.Debug("no such resource", zap.String("res", res))
	// 	return false
	// }
	//
	// // here we know, that the associated with the resource value exists
	// rr, _ := l.resources.LoadAndDelete(res)
	// r := rr.(*resource)
	//
	// if *r.lock.Load() != "" {
	// 	id := *r.lock.Load()
	// 	l.queue.Remove(id)
	// 	l.log.Debug("lock forcibly released", zap.String("lock ID", id))
	//
	// 	return true
	// }
	//
	// if r.rlocks.Len() > 0 {
	// 	ids := r.rlocks.RemoveAll()
	// 	for i := 0; i < len(ids); i++ {
	// 		l.queue.Remove(ids[i])
	// 		l.log.Debug("read lock forcibly released", zap.String("read lock ID", ids[i]))
	// 	}
	//
	// 	return true
	// }

	return false
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

	// if rr, ok := l.resources.Load(res); ok {
	// 	r := rr.(*resource)
	//
	// 	if id == "" {
	// 		if *r.lock.Load() != "" || r.rlocks.Len() > 0 {
	// 			return true
	// 		}
	//
	// 		return false
	// 	}
	//
	// 	if *r.lock.Load() == id {
	// 		return true
	// 	}
	//
	// 	if r.rlocks.Exists(id) {
	// 		return true
	// 	}
	// }

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

	// if _, ok := l.resources.Load(res); !ok {
	// 	l.log.Debug("no such resource")
	// 	return false
	// }
	//
	// // do not allow 0 ttl
	// if ttl == 0 {
	// 	return false
	// }
	//
	// // here we know, that the associated with the resource value exists
	// rr, _ := l.resources.Load(res)
	// r := rr.(*resource)
	//
	// if *r.lock.Load() == "" {
	// 	if r.rlocks.Exists(id) {
	// 		vals := l.queue.Remove(id)
	// 		// TTL expired
	// 		if len(vals) == 0 {
	// 			l.log.Debug("TTL already expired",
	// 				zap.String("resource", res),
	// 				zap.String("id", id),
	// 				zap.Int("ttl", ttl),
	// 			)
	// 			return false
	// 		}
	//
	// 		l.log.Debug("updating read-lock TTL",
	// 			zap.String("resource", res),
	// 			zap.String("id", id),
	// 			zap.Int("ttl", ttl),
	// 		)
	//
	// 		l.queue.Insert(newItem(res, id, ttl, func() {
	// 			l.log.Debug("updated rlock: ttl expired",
	// 				zap.String("id", id),
	// 				zap.Int("ttl", ttl),
	// 			)
	//
	// 			r.rlocks.Remove(id)
	// 		}))
	// 	} else {
	// 		// no lock and read lock
	// 		return false
	// 	}
	// }
	//
	// // we have lock
	// vals := l.queue.Remove(id)
	// // TTL expired
	// if len(vals) == 0 {
	// 	l.log.Debug("TTL already expired",
	// 		zap.String("resource", res),
	// 		zap.String("id", id),
	// 		zap.Int("ttl", ttl),
	// 	)
	// 	return false
	// }
	//
	// // update the TTL for the lock
	// l.log.Debug("updating lock TTL",
	// 	zap.String("resource", res),
	// 	zap.String("id", id),
	// 	zap.Int("ttl", ttl),
	// )
	//
	// l.queue.Insert(newItem(res, id, ttl, func() {
	// 	l.log.Debug("lock: ttl expired",
	// 		zap.String("id", id),
	// 		zap.Int("ttl", ttl),
	// 	)
	// 	r.lock.Store(ptrTo(""))
	// }))

	return true
}

func (l *locker) stop() {
	l.stopCh <- struct{}{}
}

func ptrTo[T any](val T) *T {
	return &val
}

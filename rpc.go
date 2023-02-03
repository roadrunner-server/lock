package lock

import (
	"sync"
)

type rpc struct {
	mu sync.Mutex
	pl *Plugin
}

func (r *rpc) Lock(name string, hash string, ttl int) error {
	return nil
}

func (r *rpc) LockRead() {}

func (r *rpc) UnLock(name string, hash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return nil
}

func (r *rpc) ForceUnlock() {}
func (r *rpc) Exists()      {}

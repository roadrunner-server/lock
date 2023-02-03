package rlocks

import (
	"sync"
)

type RLocks struct {
	mu        sync.Mutex
	readLocks []string
}

func NewRLocks() *RLocks {
	return &RLocks{
		readLocks: make([]string, 0, 10),
	}
}

func (rl *RLocks) Len() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	return len(rl.readLocks)
}

func (rl *RLocks) Exists(id string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// TODO(rustatian): dumb algo, update to BST to search easily (or map)
	for i := 0; i < len(rl.readLocks); i++ {
		// got the needed read-lock
		if rl.readLocks[i] == id {
			return true
		}
	}

	return false
}

func (rl *RLocks) Append(id string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.readLocks = append(rl.readLocks, id)
}

func (rl *RLocks) Remove(id string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// TODO(rustatian): dumb algo, update to BST to search easily (or map)
	for i := 0; i < len(rl.readLocks); i++ {
		// got the needed read-lock
		if rl.readLocks[i] == id {
			// remove the element, do not preserve the order
			rl.readLocks[i] = rl.readLocks[len(rl.readLocks)-1]
			rl.readLocks = rl.readLocks[:len(rl.readLocks)-1]
			return true
		}
	}

	return false
}

func (rl *RLocks) RemoveAll() []string {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	ret := make([]string, len(rl.readLocks))
	copy(ret, rl.readLocks)
	rl.readLocks = nil

	return ret
}

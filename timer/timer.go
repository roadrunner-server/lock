package timer

import (
	"sync"
	"time"
)

type timer struct {
	tm     *time.Timer
	stopCh chan struct{}
}

type Timers struct {
	mu     sync.Mutex
	timers sync.Map //resourceID -> timer
}

func New() *Timers {
	return &Timers{
		mu: sync.Mutex{},
	}
}

func (t *Timers) Stop(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	val, ok := t.timers.LoadAndDelete(id)
	if !ok {
		// timer already deleted or there is no such timer
		return
	}

	tt := val.(*timer)
	tt.stopCh <- struct{}{}

	if !tt.tm.Stop() {
		tt.tm.Reset(time.Nanosecond)
		<-tt.tm.C
	}
}

func (t *Timers) StopAll() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.timers.Range(func(key, value any) bool {
		id := key.(string)
		_, ok := t.timers.LoadAndDelete(id)
		if !ok {
			return true
		}

		tt := value.(*timer)
		tt.stopCh <- struct{}{}

		if !tt.tm.Stop() {
			tt.tm.Reset(time.Nanosecond)
			<-tt.tm.C
		}

		return true
	})
}

func (t *Timers) UpdateTTL(id string, newTTL int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// if the timer exists
	if val, ok := t.timers.Load(id); ok {
		tt := val.(*timer)
		tt.tm.Reset(time.Second * time.Duration(newTTL))
	}
}

func (t *Timers) StoreAndWait(id string, newT *time.Timer, onExpire func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// check for the existing timer
	if tmr, ok := t.timers.LoadAndDelete(id); ok {
		oldT := tmr.(*timer)
		oldT.stopCh <- struct{}{}
	}

	tCh := &timer{
		tm:     newT,
		stopCh: make(chan struct{}, 1),
	}

	// store the timer
	t.timers.Store(id, tCh)

	go func() {
		for {
			select {
			case <-tCh.tm.C:
				onExpire()
				return
			case <-tCh.stopCh:
				if !tCh.tm.Stop() {
					tCh.tm.Reset(time.Nanosecond)
					<-tCh.tm.C
				}
				return
			}
		}
	}()
}

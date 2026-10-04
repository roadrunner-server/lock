package lock

import (
	"io"
	"log/slog"
	"testing"
)

func TestForceReleaseRegisteredLockWithZeroCounters(t *testing.T) {
	l := newLocker(slog.New(slog.NewTextHandler(io.Discard, nil)))
	name := t.Name()
	r := &resource{
		resourceMu:     newResLock(l.log),
		notificationCh: make(chan struct{}, 1),
		stopCh:         make(chan struct{}, 1),
	}
	l.resources[name] = r

	// Writer cleanup can clear the counters after a new reader registers.
	cleanup := l.makeLockCallback(r, name, "reader", 0)
	registered, ok := r.locks.Load("reader")
	if !ok {
		t.Fatal("the lock must be registered before its callback runs")
	}
	stopCh := registered.(*item).stopCh

	if !l.forceRelease(t.Context(), name) {
		t.Fatal("force release must remove a registered live lock with zero counters")
	}
	if len(stopCh) != 1 {
		t.Fatal("force release did not signal the registered lock")
	}

	// The lock stays registered while its release signal is queued.
	if !l.forceRelease(t.Context(), name) {
		t.Fatal("a registered lock with a pending release is still live")
	}

	cleanup(r.notificationCh, r.stopCh)
	if l.exists(t.Context(), name, "reader") {
		t.Fatal("force release did not remove the registered lock")
	}
	if l.forceRelease(t.Context(), name) {
		t.Fatal("the removed lock is not a live lock")
	}
}

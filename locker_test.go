package lock

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// longTTL is the TTL, in microseconds, used where a lock must outlive the test.
// The plugin's own unlimited window is a zero TTL, which is a different case.
const longTTL = int(time.Hour / time.Microsecond)

// TestLockerGlobalMutexBusy checks that every entry point gives up when the
// global mutex cannot be taken within the caller's deadline.
func TestLockerGlobalMutexBusy(t *testing.T) {
	l := newLocker(slog.New(slog.DiscardHandler))

	require.True(t, l.globalMu.lock(t.Context()))

	// the caller's deadline is what ends the wait for the mutex
	shortCtx := func() context.Context {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond*10)
		t.Cleanup(cancel)

		return ctx
	}

	require.False(t, l.lock(shortCtx(), "res", "id", longTTL))
	require.False(t, l.lockRead(shortCtx(), "res", "id", longTTL))
	require.False(t, l.release(shortCtx(), "res", "id"))
	require.False(t, l.forceRelease(shortCtx(), "res"))
	require.False(t, l.exists(shortCtx(), "res", "id"))
	require.False(t, l.updateTTL(shortCtx(), "res", "id", longTTL))

	l.globalMu.unlock()

	// nothing was recorded while the mutex was busy, the locker still works
	require.False(t, l.exists(t.Context(), "res", "id"))
	require.True(t, l.lock(t.Context(), "res", "id", longTTL))
}

// TestLockerStopGlobalMutexBusy checks that stop leaves the resources alone when
// it cannot take the global mutex.
func TestLockerStopGlobalMutexBusy(t *testing.T) {
	l := newLocker(slog.New(slog.DiscardHandler))

	require.True(t, l.lock(t.Context(), "res", "id", longTTL))
	require.True(t, l.globalMu.lock(t.Context()))

	stopCtx, cancel := context.WithTimeout(t.Context(), time.Millisecond*10)
	defer cancel()
	l.stop(stopCtx)

	l.globalMu.unlock()

	require.True(t, l.exists(t.Context(), "res", "id"))
}

// TestLockerUnknownResourceAndID checks the not-found paths of every lookup, for
// a resource that was never locked and for an id that never held a lock.
func TestLockerUnknownResourceAndID(t *testing.T) {
	l := newLocker(slog.New(slog.DiscardHandler))
	ctx := t.Context()

	require.False(t, l.release(ctx, "nope", "id"))
	require.False(t, l.forceRelease(ctx, "nope"))
	require.False(t, l.updateTTL(ctx, "nope", "id", longTTL))
	require.False(t, l.exists(ctx, "nope", "id"))
	require.False(t, l.exists(ctx, "nope", "*"))

	require.True(t, l.lock(ctx, "owned", "owner", longTTL))
	require.False(t, l.release(ctx, "owned", "other"))
	require.False(t, l.updateTTL(ctx, "owned", "other", longTTL))
	require.False(t, l.exists(ctx, "owned", "other"))

	// a read lock leaves no owner, so the id lookup is what rejects the release
	require.True(t, l.lockRead(ctx, "shared", "reader", longTTL))
	require.False(t, l.release(ctx, "shared", "ghost"))
}

// TestLockerReacquireAfterExpiry checks that a resource whose locks have all
// expired can be locked again.
func TestLockerReacquireAfterExpiry(t *testing.T) {
	l := newLocker(slog.New(slog.DiscardHandler))
	ctx := t.Context()

	require.True(t, l.lock(ctx, "res", "first", int(time.Millisecond*100/time.Microsecond)))
	require.Eventually(t, func() bool {
		return !l.exists(ctx, "res", "*")
	}, time.Second*10, time.Millisecond*10, "the lock never expired")

	require.True(t, l.lock(ctx, "res", "second", longTTL))
	require.True(t, l.exists(ctx, "res", "second"))
}

// TestLockerUpdateTTLZeroIsUnlimited checks that a zero TTL replaces a finite one
// with the unlimited window instead of expiring the lock right away.
func TestLockerUpdateTTLZeroIsUnlimited(t *testing.T) {
	l := newLocker(slog.New(slog.DiscardHandler))
	ctx := t.Context()

	require.True(t, l.lock(ctx, "res", "id", int(time.Millisecond*200/time.Microsecond)))
	require.True(t, l.updateTTL(ctx, "res", "id", 0))

	require.Never(t, func() bool {
		return !l.exists(ctx, "res", "id")
	}, time.Millisecond*500, time.Millisecond*20, "the lock expired on its original TTL")
}

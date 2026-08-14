package lock

import (
	"testing"
	"time"

	"tests/helpers"

	"github.com/stretchr/testify/require"
)

// TestLockUpdateTTL checks that a new TTL replaces the one the lock was taken
// with: a reader waiting on the writer acquires the resource once the shortened
// TTL expires, long before the original one would have.
func TestLockUpdateTTL(t *testing.T) {
	rr, cl := startLock(t, helpers.WithObservedLogger())

	acquired, err := cl.Lock("foo", "bar", time.Hour, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	updated, err := cl.UpdateTTL("foo", "bar", time.Millisecond*200)
	require.NoError(t, err)
	require.True(t, updated)

	acquired, err = cl.LockRead("foo", "bar1", 0, time.Second*10)
	require.NoError(t, err)
	require.True(t, acquired)

	released, err := cl.Release("foo", "bar1")
	require.NoError(t, err)
	require.True(t, released)

	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("updateTTL request received").Len())
	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("r/lock: ttl was updated").Len())
	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("lock successfully released").Len())

	// the callback logs the stop after release returns
	require.Eventually(t, func() bool {
		return rr.Logs.FilterMessageSnippet("r/lock: ttl removed, stop callback call").Len() == 1
	}, time.Second*10, time.Millisecond*20)
}

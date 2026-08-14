package lock

import (
	"testing"
	"time"

	"tests/helpers"

	"github.com/stretchr/testify/require"
)

// TestForceRelease checks that force-release drops the writer's lock, so a
// reader that could not acquire the resource before now can.
func TestForceRelease(t *testing.T) {
	rr, cl := startLock(t, helpers.WithObservedLogger())

	acquired, err := cl.Lock("foo", "bar", time.Hour, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	acquired, err = cl.LockRead("foo", "bar1", 0, time.Millisecond*200)
	require.NoError(t, err)
	require.False(t, acquired)

	forced, err := cl.ForceRelease("foo")
	require.NoError(t, err)
	require.True(t, forced)

	acquired, err = cl.LockRead("foo", "bar1", 0, time.Second*10)
	require.NoError(t, err)
	require.True(t, acquired)

	held, err := cl.Exists("foo", "bar1")
	require.NoError(t, err)
	require.True(t, held)

	released, err := cl.Release("foo", "bar1")
	require.NoError(t, err)
	require.True(t, released)

	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("failed to acquire a readlock, timeout exceeded, w==1, r==0").Len())
	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("all force-release messages were sent").Len())
	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("lock successfully released").Len())

	// one callback stops on the force release, the other on the explicit release
	require.Eventually(t, func() bool {
		return rr.Logs.FilterMessageSnippet("r/lock: ttl removed, stop callback call").Len() == 2
	}, time.Second*10, time.Millisecond*20)
}

// TestExistsWildcard checks the wildcard id, which reports whether a resource
// holds any lock at all.
func TestExistsWildcard(t *testing.T) {
	_, cl := startLock(t)

	acquired, err := cl.Lock("wild", "Y", time.Hour, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	held, err := cl.Exists("wild", "*")
	require.NoError(t, err)
	require.True(t, held)

	held, err = cl.Exists("absent", "*")
	require.NoError(t, err)
	require.False(t, held)

	released, err := cl.Release("wild", "Y")
	require.NoError(t, err)
	require.True(t, released)

	// the resource outlives its locks, the wildcard follows the counters
	require.Eventually(t, func() bool {
		anyHeld, errE := cl.Exists("wild", "*")
		return errE == nil && !anyHeld
	}, time.Second*10, time.Millisecond*20)
}

// TestReleaseUnknownResource checks the not-found paths of the release family.
func TestReleaseUnknownResource(t *testing.T) {
	_, cl := startLock(t)

	released, err := cl.Release("nope", "id")
	require.NoError(t, err)
	require.False(t, released)

	forced, err := cl.ForceRelease("nope")
	require.NoError(t, err)
	require.False(t, forced)

	updated, err := cl.UpdateTTL("nope", "id", time.Second)
	require.NoError(t, err)
	require.False(t, updated)

	held, err := cl.Exists("nope", "id")
	require.NoError(t, err)
	require.False(t, held)
}

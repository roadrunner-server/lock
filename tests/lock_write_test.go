package lock

import (
	"sync"
	"testing"
	"time"

	"tests/helpers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLockDifferentIDs checks that a release issued by an id other than the
// holder is rejected.
func TestLockDifferentIDs(t *testing.T) {
	rr, cl := startLock(t, helpers.WithObservedLogger())

	acquired, err := cl.Lock("foo", "bar", time.Minute, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	released, err := cl.Release("foo", "bar1")
	require.NoError(t, err)
	require.False(t, released)

	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("lock request received").Len())
	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("release called for the resource which is not owned by the caller").Len())
}

// TestLockFromSeveralProcesses checks that a single writer out of several
// concurrent ones takes the resource and the rest give up on their wait.
func TestLockFromSeveralProcesses(t *testing.T) {
	_, cl := startLock(t)

	// The holder's TTL outlives the contenders' wait, so the outcome does not
	// depend on which of them reaches the plugin first.
	acquiredBy := make([]bool, 4)

	wg := &sync.WaitGroup{}
	for i := range acquiredBy {
		wg.Go(func() {
			acquired, err := cl.Lock("foo", "bar", time.Minute, time.Millisecond*200)
			assert.NoError(t, err)
			acquiredBy[i] = acquired
		})
	}
	wg.Wait()

	holders := 0
	for _, acquired := range acquiredBy {
		if acquired {
			holders++
		}
	}

	require.Equal(t, 1, holders)
}

// TestLockWaitThenAcquire checks the wait-then-acquire arm: a second writer
// blocks on the notification channel and takes the lock once the holder's TTL
// expires, instead of timing out.
func TestLockWaitThenAcquire(t *testing.T) {
	_, cl := startLock(t)

	acquired, err := cl.Lock("wta", "A", time.Millisecond*500, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	acquired, err = cl.Lock("wta", "B", time.Minute, time.Second*10)
	require.NoError(t, err)
	require.True(t, acquired, "B should acquire the lock after A expires")
}

// TestLockAfterAllLocksExpired checks that a resource whose locks have all
// expired can be locked again.
func TestLockAfterAllLocksExpired(t *testing.T) {
	_, cl := startLock(t)

	acquired, err := cl.Lock("expired", "A", time.Millisecond*100, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	require.Eventually(t, func() bool {
		held, errE := cl.Exists("expired", "*")
		return errE == nil && !held
	}, time.Second*10, time.Millisecond*20, "the lock never expired")

	acquired, err = cl.Lock("expired", "B", time.Minute, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	held, err := cl.Exists("expired", "B")
	require.NoError(t, err)
	require.True(t, held)
}

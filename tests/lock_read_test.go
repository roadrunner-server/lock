package lock

import (
	"sync"
	"testing"
	"time"

	"tests/helpers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLockReadInit checks that a read lock waits out the writer, that further
// readers join it, and that each holder can release its own lock.
func TestLockReadInit(t *testing.T) {
	rr, cl := startLock(t, helpers.WithObservedLogger())

	acquired, err := cl.Lock("foo", "bar", time.Millisecond*500, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	// blocks until the writer's TTL expires, then holds the resource as a reader
	acquired, err = cl.LockRead("foo", "bar", 0, time.Second*10)
	require.NoError(t, err)
	require.True(t, acquired)

	joiners := []string{"bar1", "bar2"}

	wg := &sync.WaitGroup{}
	for _, id := range joiners {
		wg.Go(func() {
			joined, errR := cl.LockRead("foo", id, 0, time.Second*10)
			assert.NoError(t, errR)
			assert.True(t, joined)
		})
	}
	wg.Wait()

	for _, id := range joiners {
		held, errE := cl.Exists("foo", id)
		require.NoError(t, errE)
		require.True(t, held)
	}

	for _, id := range []string{"bar", "bar1", "bar2"} {
		released, errR := cl.Release("foo", id)
		require.NoError(t, errR)
		require.True(t, released)
	}

	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("no such lock resource, creating new").Len())
	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("waiting to acquire a lock, w==1, r==0").Len())
	require.Equal(t, 2, rr.Logs.FilterMessageSnippet("exists request received").Len())
	require.Equal(t, 3, rr.Logs.FilterMessageSnippet("lock successfully released").Len())
	require.Equal(t, 1, rr.Logs.FilterMessageSnippet("returning releaseMuCh mutex to temporarily allow releasing locks").Len())
}

// TestLockPromoteReadToWrite checks that the only reader of a resource can take
// the write lock under the same id.
func TestLockPromoteReadToWrite(t *testing.T) {
	_, cl := startLock(t)

	acquired, err := cl.LockRead("promote", "X", time.Minute, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	// the promotion arm signals the reader to stop, waits for the notification,
	// then takes the write lock
	acquired, err = cl.Lock("promote", "X", time.Minute, time.Second*10)
	require.NoError(t, err)
	require.True(t, acquired)
}

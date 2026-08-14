package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCallsWithoutWait covers the budget a client that sends no wait field gets:
// the plugin bounds such a call by its own immediate timeout, which is short
// enough that a call can miss it while the locker is busy elsewhere. Each method
// is retried until one lands, so what is asserted is that the call reaches the
// locker at all, not how fast it does.
func TestCallsWithoutWait(t *testing.T) {
	_, cl := startLock(t)

	acquired, err := cl.Lock("nowait", "id", time.Hour, time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	// The order follows the lock taken above: the lookups run while it is held,
	// the release drops it, and force-release then acts on the bare resource.
	methods := []string{"lock.Exists", "lock.UpdateTTL", "lock.Release", "lock.ForceRelease"}

	for _, method := range methods {
		require.Eventually(t, func() bool {
			ok, errC := cl.CallWithoutWait(method, "nowait", "id")
			return errC == nil && ok
		}, time.Second*10, time.Millisecond*20, "%s never answered", method)
	}
}

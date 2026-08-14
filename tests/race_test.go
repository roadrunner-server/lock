package lock

import (
	"crypto/rand"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentLockOperations hammers every rpc method against a handful of
// resources from many goroutines at once, so the race detector sees the locker
// under contention. Only the transport is asserted: whether an individual call
// wins its resource depends on timing by design.
func TestConcurrentLockOperations(t *testing.T) {
	_, cl := startLock(t)

	resources := []string{"foo", "foo1", "foo2", "foo3", "foo4", "foo5"}
	resource := func() string { return resources[genRandNum(len(resources))] }

	// The TTL and wait windows stay in the millisecond range so the acquire,
	// expire and notify paths all cycle many times within the run.
	window := func(upper int) time.Duration {
		return time.Duration(genRandNum(upper)+1) * time.Millisecond
	}

	// The update window starts at zero, the value the plugin turns into an
	// unlimited TTL, so that substitution happens under contention too.
	updateWindow := func(upper int) time.Duration {
		return time.Duration(genRandNum(upper)) * time.Millisecond
	}

	// One round of calls, spread over every method the plugin exposes. The id
	// argument is shared within a round, so releases and lookups sometimes hit a
	// lock another call in the same round took.
	round := []func(id string) (bool, error){
		func(id string) (bool, error) { return cl.Lock(resource(), id, window(5), window(15)) },
		func(string) (bool, error) { return cl.Lock(resource(), randomString(3), window(4), window(11)) },
		func(string) (bool, error) { return cl.Lock(resource(), randomString(3), window(2), window(90)) },
		func(string) (bool, error) { return cl.Lock(resource(), randomString(3), window(10), window(10)) },
		func(string) (bool, error) { return cl.Lock(resource(), randomString(3), window(20), window(13)) },
		func(string) (bool, error) { return cl.Lock(resource(), randomString(3), window(80), window(10)) },
		func(string) (bool, error) { return cl.Lock(resource(), randomString(3), window(20), window(19)) },
		func(string) (bool, error) { return cl.LockRead(resource(), randomString(3), window(20), window(15)) },
		func(string) (bool, error) { return cl.LockRead(resource(), randomString(3), window(2), window(34)) },
		func(string) (bool, error) { return cl.LockRead(resource(), randomString(3), window(20), window(13)) },
		func(string) (bool, error) { return cl.LockRead(resource(), randomString(3), window(25), window(15)) },
		func(string) (bool, error) { return cl.LockRead(resource(), randomString(3), window(20), window(76)) },
		func(string) (bool, error) { return cl.LockRead(resource(), randomString(3), window(20), window(15)) },
		func(string) (bool, error) { return cl.UpdateTTL(resource(), randomString(3), updateWindow(5)) },
		func(id string) (bool, error) { return cl.Exists(resource(), id) },
		func(id string) (bool, error) { return cl.Release(resource(), id) },
		func(string) (bool, error) { return cl.ForceRelease(resource()) },
	}

	wg := &sync.WaitGroup{}
	for range 100 {
		id := randomString(10)

		for _, call := range round {
			wg.Go(func() {
				_, err := call(id)
				assert.NoError(t, err)
			})
		}
	}
	wg.Wait()

	// the plugin still serves a full lock/release round trip afterwards
	acquired, err := cl.Lock("after-storm", "id", time.Minute, time.Second*10)
	require.NoError(t, err)
	require.True(t, acquired)

	released, err := cl.Release("after-storm", "id")
	require.NoError(t, err)
	require.True(t, released)
}

const letterBytes = "abc"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[genRandNum(len(letterBytes))]
	}

	return string(b)
}

func genRandNum(upper int) int {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(upper)))
	if err != nil {
		panic(err)
	}

	return int(n.Int64())
}

package lock

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	operationSemaphoreFull = "failed to put operation semaphore back, channel is full"
	releaseSemaphoreFull   = "failed to put release semaphore back, channel is full"
)

// logCapture is a concurrency-safe sink for slog output.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.buf.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.buf.String()
}

// newCaptureLogger returns a debug-level logger and the sink holding its output.
func newCaptureLogger() (*slog.Logger, *logCapture) {
	c := &logCapture{}

	return slog.New(slog.NewTextHandler(c, &slog.HandlerOptions{Level: slog.LevelDebug})), c
}

// TestResLockCtxExpiryRestoresOperationSemaphore checks that a lock giving up
// while it waits for the release semaphore puts the operation semaphore back.
func TestResLockCtxExpiryRestoresOperationSemaphore(t *testing.T) {
	log, captured := newCaptureLogger()
	rl := newResLock(log)

	// somebody else owns the release semaphore
	require.True(t, rl.lockRelease(t.Context()))

	waitsForRelease, cancelRelease := context.WithTimeout(t.Context(), time.Millisecond*10)
	defer cancelRelease()
	require.False(t, rl.lock(waitsForRelease))

	rl.unlockRelease()

	// the operation semaphore is back, so a full lock succeeds exactly once
	require.True(t, rl.lock(t.Context()))

	waitsForOperation, cancelOperation := context.WithTimeout(t.Context(), time.Millisecond*10)
	defer cancelOperation()
	require.False(t, rl.lock(waitsForOperation))

	require.NotContains(t, captured.String(), "channel is full")
}

// TestResLockUnlockGuards checks that unlocking an already armed semaphore is
// reported and leaks no token.
func TestResLockUnlockGuards(t *testing.T) {
	log, captured := newCaptureLogger()
	rl := newResLock(log)

	require.True(t, rl.lock(t.Context()))
	rl.unlockRelease()
	// only the operation semaphore is missing, the release one is already back
	rl.unlock()

	rl.unlock()
	rl.unlockOperation()
	rl.unlockRelease()

	require.Equal(t, 2, strings.Count(captured.String(), operationSemaphoreFull))
	require.Equal(t, 2, strings.Count(captured.String(), releaseSemaphoreFull))

	// both semaphores hold exactly one token
	require.True(t, rl.lock(t.Context()))

	waitsForOperation, cancel := context.WithTimeout(t.Context(), time.Millisecond*10)
	defer cancel()
	require.False(t, rl.lock(waitsForOperation))
}

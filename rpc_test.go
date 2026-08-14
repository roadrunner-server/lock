package lock

import (
	"context"
	"log/slog"
	"testing"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/stretchr/testify/require"
)

// loggerProvider satisfies the Logger dependency Plugin.Init asks for.
type loggerProvider struct {
	log *slog.Logger
}

func (p loggerProvider) NamedLogger(string) *slog.Logger { return p.log }

// micros expresses a duration in the microseconds the lock protocol carries.
func micros(d time.Duration) *int64 {
	return new(int64(d / time.Microsecond))
}

// newPluginRPC boots the plugin through its real surface and returns the rpc
// facade the rpc plugin would serve.
func newPluginRPC(t *testing.T) *rpc {
	t.Helper()

	p := &Plugin{}
	require.NoError(t, p.Init(loggerProvider{log: slog.New(slog.DiscardHandler)}))

	// the name prefixes every rpc method the clients call, and endure serves the
	// higher weights first
	require.Equal(t, "lock", p.Name())
	require.Equal(t, uint(100), p.Weight())

	errs := p.Serve()

	t.Cleanup(func() {
		require.NoError(t, p.Stop(context.Background()))

		select {
		case err := <-errs:
			require.NoError(t, err)
		default:
		}
	})

	r, ok := p.RPC().(*rpc)
	require.True(t, ok)

	return r
}

// TestRPCEmptyIDRejected checks the id guard every per-holder method carries.
func TestRPCEmptyIDRejected(t *testing.T) {
	r := newPluginRPC(t)

	guarded := []struct {
		name string
		call func(*lockV1.Request, *lockV1.Response) error
	}{
		{name: "Lock", call: r.Lock},
		{name: "LockRead", call: r.LockRead},
		{name: "Release", call: r.Release},
		{name: "Exists", call: r.Exists},
		{name: "UpdateTTL", call: r.UpdateTTL},
	}

	for _, g := range guarded {
		t.Run(g.name, func(t *testing.T) {
			out := &lockV1.Response{}
			require.ErrorIs(t, g.call(&lockV1.Request{Resource: "res"}, out), errEmptyID)
			require.False(t, out.GetOk())
		})
	}
}

// TestRPCForceReleaseWithoutID pins the asymmetry of the id guard: force-release
// acts on the whole resource, so it takes no id and rejects none.
func TestRPCForceReleaseWithoutID(t *testing.T) {
	r := newPluginRPC(t)

	out := &lockV1.Response{}
	require.NoError(t, r.Lock(&lockV1.Request{
		Resource: "res",
		Id:       "holder",
		Ttl:      micros(time.Minute),
		Wait:     micros(time.Second),
	}, out))
	require.True(t, out.GetOk())

	out = &lockV1.Response{}
	require.NoError(t, r.ForceRelease(&lockV1.Request{
		Resource: "res",
		Wait:     micros(time.Second),
	}, out))
	require.True(t, out.GetOk())
}

// TestWaitContextUnits pins the microsecond unit of the wait field and the
// fallback every method relies on when the caller sends no wait.
func TestWaitContextUnits(t *testing.T) {
	cases := []struct {
		name    string
		waitUs  int64
		timeout time.Duration
	}{
		{name: "unset wait falls back to the immediate timeout", waitUs: 0, timeout: defaultImmediateTimeout},
		{name: "wait is microseconds", waitUs: 5000, timeout: time.Millisecond * 5},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			start := time.Now()
			ctx, cancel := waitContext(context.Background(), c.waitUs)
			defer cancel()

			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinRange(t, deadline, start.Add(c.timeout), start.Add(c.timeout+time.Millisecond*100))
		})
	}
}

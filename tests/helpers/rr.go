package helpers

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	mocklogger "tests/mock"

	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/logger/v6"
	"github.com/stretchr/testify/require"
)

const (
	// configVersion is the config schema version used by the test configs.
	configVersion = "2023.3.5"
	// containerLogLevel keeps endure's own logging out of the test output.
	containerLogLevel = slog.LevelError
	// probeTimeout caps how long Start waits for the server to answer the probe.
	probeTimeout = time.Second * 15
	probeTick    = time.Millisecond * 20
	probeDial    = time.Second
)

// bootCfg holds the options applied to a container before it is started.
type bootCfg struct {
	logger loggerKind
	probe  func(ctx context.Context) bool
}

// loggerKind selects which logger plugin Start registers.
type loggerKind int

const (
	realLogger loggerKind = iota
	observedLogger
)

// Option customizes the container built by Start.
type Option func(*bootCfg)

// WithObservedLogger registers an in-memory logger instead of the real logger
// plugin and exposes the captured records as RR.Logs.
func WithObservedLogger() Option {
	return func(b *bootCfg) { b.logger = observedLogger }
}

// WithTCPProbe makes Start return only once addr accepts a connection. The rpc
// listener binds during Serve, so this proves readiness without issuing a call
// that would show up in the observed logs.
func WithTCPProbe(addr string) Option {
	return func(b *bootCfg) {
		b.probe = func(ctx context.Context) bool {
			d := net.Dialer{Timeout: probeDial}
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return false
			}

			_ = conn.Close()
			return true
		}
	}
}

// RR is a running container.
type RR struct {
	// Logs holds the captured log records, non-nil only with WithObservedLogger.
	Logs *mocklogger.ObservedLogs
}

// Start registers the plugins, boots the container and waits for the probe, if
// any, to answer. Errors arriving on the container channel are reported through
// t.Errorf and stop the container, but they do not abort the test.
//
// The returned stop is idempotent and also registered with t.Cleanup, so tests
// asserting on logs written during shutdown can stop the container mid-test.
func Start(t *testing.T, cfgPath string, plugins []any, opts ...Option) (*RR, func()) {
	t.Helper()

	cont, rr, bc := newContainer(t, cfgPath, plugins, opts)
	require.NoError(t, cont.Init())

	ch, err := cont.Serve()
	require.NoError(t, err)

	stopCont := sync.OnceValue(cont.Stop)
	done := make(chan struct{})
	wg := &sync.WaitGroup{}

	wg.Go(func() {
		for {
			select {
			case res := <-ch:
				if res == nil {
					return
				}
				t.Errorf("plugin %s reported an error: %v", res.VertexID, res.Error)
				if errS := stopCont(); errS != nil {
					t.Errorf("container stop: %v", errS)
				}
			case <-done:
				if errS := stopCont(); errS != nil {
					t.Errorf("container stop: %v", errS)
				}
				return
			}
		}
	})

	// The drain goroutine calls t.Errorf, so it has to be joined while the test
	// is still running.
	stop := sync.OnceFunc(func() {
		close(done)
		wg.Wait()
	})
	t.Cleanup(stop)

	if bc.probe != nil {
		require.Eventually(t, func() bool { return bc.probe(t.Context()) }, probeTimeout, probeTick, "server did not become ready")
	}

	return rr, stop
}

// newContainer builds the container and registers the config, a logger and the
// caller's plugins. The container is not initialized yet.
func newContainer(t *testing.T, cfgPath string, plugins []any, opts []Option) (*endure.Endure, *RR, *bootCfg) {
	t.Helper()

	bc := &bootCfg{}
	for _, o := range opts {
		o(bc)
	}

	rr := &RR{}
	all := []any{&config.Plugin{Version: configVersion, Path: cfgPath}}

	switch bc.logger {
	case realLogger:
		all = append(all, &logger.Plugin{})
	case observedLogger:
		l, obs := mocklogger.SlogTestLogger(slog.LevelDebug)
		rr.Logs = obs
		all = append(all, l)
	}

	cont := endure.New(containerLogLevel)
	require.NoError(t, cont.RegisterAll(append(all, plugins...)...))

	return cont, rr, bc
}

package lock

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/roadrunner-server/api-go/v6/lock/v1/lockV1connect"
)

const pluginName string = "lock"

// Logger plugin
type Logger interface {
	NamedLogger(name string) *slog.Logger
}

type Plugin struct {
	log   *slog.Logger
	locks *locker
}

func (p *Plugin) Init(log Logger) error {
	p.log = log.NamedLogger(pluginName)
	p.locks = newLocker(p.log)
	return nil
}

func (p *Plugin) Serve() chan error {
	return make(chan error, 1)
}

func (p *Plugin) Stop(ctx context.Context) error {
	p.locks.stop(ctx)
	return nil
}

func (p *Plugin) Weight() uint {
	return 100
}

func (p *Plugin) Name() string {
	return pluginName
}

// RPC returns the Connect-RPC service handler for lock.v1.LockService.
// The rpc plugin mounts the returned handler at the returned path on its HTTP/2 mux.
func (p *Plugin) RPC() (string, http.Handler) {
	return lockV1connect.NewLockServiceHandler(&rpc{pl: p, log: p.log})
}

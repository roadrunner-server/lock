package lock

import (
	"context"

	"go.uber.org/zap"
)

const pluginName string = "lock"

// Logger plugin
type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Plugin struct {
	log   *zap.Logger
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

func (p *Plugin) RPC() any {
	return &rpc{
		log: p.log,
		pl:  p,
	}
}

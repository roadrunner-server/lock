package lock

import (
	"sync"

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

func (p *Plugin) Stop() error {
	p.locks.stop()
	return nil
}

func (p *Plugin) Weight() uint {
	return 80
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) RPC() any {
	return &rpc{
		log: p.log,
		mu:  sync.Mutex{},
		pl:  p,
	}
}

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
	locks sync.Map // string -> *locker
}

func (p *Plugin) Init(log Logger) error {
	p.log = log.NamedLogger(pluginName)
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) RPC() any {
	return nil
}

package lock

import (
	"context"
	"fmt"
	"log/slog"
)

const pluginName string = "lock"

// Logger plugin
type Logger interface {
	NamedLogger(name string) *slog.Logger
}

type Configurer interface {
	Has(name string) bool
	UnmarshalKey(name string, out any) error
}

type backend interface {
	lock(ctx context.Context, res, id string, ttl, wait int64) (bool, error)
	lockRead(ctx context.Context, res, id string, ttl, wait int64) (bool, error)
	release(ctx context.Context, res, id string, wait int64) (bool, error)
	forceRelease(ctx context.Context, res string, wait int64) (bool, error)
	exists(ctx context.Context, res, id string, wait int64) (bool, error)
	updateTTL(ctx context.Context, res, id string, ttl, wait int64) (bool, error)
	stop(ctx context.Context) error
}

type Plugin struct {
	log   *slog.Logger
	locks backend
}

func (p *Plugin) Init(cfg Configurer, log Logger) error {
	p.log = log.NamedLogger(pluginName)
	if !cfg.Has(pluginName) {
		p.locks = &memoryBackend{locker: newLocker(p.log)}
		p.log.Info("lock backend initialized", "driver", "memory")
		return nil
	}

	var conf Config
	if err := cfg.UnmarshalKey(pluginName, &conf); err != nil {
		return fmt.Errorf("lock configuration: %w", err)
	}
	switch conf.Driver {
	case "memory":
		p.locks = &memoryBackend{locker: newLocker(p.log)}
		p.log.Info("lock backend initialized", "driver", "memory")
		return nil
	case "redis":
		locks, err := newRedisBackend(p.log, conf.Config)
		if err != nil {
			return fmt.Errorf("lock redis: %w", err)
		}
		p.locks = locks
		return nil
	default:
		return fmt.Errorf("unsupported lock driver: %q", conf.Driver)
	}
}

func (p *Plugin) Serve() chan error {
	return make(chan error, 1)
}

func (p *Plugin) Stop(ctx context.Context) error {
	return p.locks.stop(ctx)
}

func (p *Plugin) Weight() uint {
	return 100
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) RPC() any {
	return &rpc{pl: p}
}

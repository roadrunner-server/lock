package lock

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisDuplicateReadDelay is the Lua sentinel for terminal read refusal.
const redisDuplicateReadDelay = -2 * time.Microsecond

//go:embed redis.lua
var redisScript string

type redisBackend struct {
	client redis.UniversalClient
	script *redis.Script
	ctx    context.Context
	cancel context.CancelFunc
}

func newRedisBackend(cfg RedisConfig) (*redisBackend, error) {
	cfg.InitDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	tlsConf, err := tlsConfig(cfg.TLSConfig)
	if err != nil {
		return nil, err
	}
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:                 cfg.Addrs,
		Username:              cfg.Username,
		Password:              cfg.Password,
		DB:                    cfg.DB,
		MasterName:            cfg.MasterName,
		SentinelPassword:      cfg.SentinelPassword,
		PoolSize:              cfg.PoolSize,
		TLSConfig:             tlsConf,
		DialTimeout:           cfg.DialTimeout,
		ReadTimeout:           cfg.ReadTimeout,
		WriteTimeout:          cfg.WriteTimeout,
		ContextTimeoutEnabled: true,
		// A lost acquisition reply must reach the caller as an error.
		MaxRetries: -1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		_ = client.Close()
		return nil, err
	}
	return &redisBackend{client: client, script: redis.NewScript(redisScript), ctx: ctx, cancel: cancel}, nil
}

func (r *redisBackend) lock(ctx context.Context, res, id string, ttl, wait int64) (bool, error) {
	return r.acquire(ctx, "lock", res, id, ttl, wait)
}

func (r *redisBackend) lockRead(ctx context.Context, res, id string, ttl, wait int64) (bool, error) {
	return r.acquire(ctx, "read", res, id, ttl, wait)
}

func (r *redisBackend) release(ctx context.Context, res, id string, wait int64) (bool, error) {
	return r.operate(ctx, "release", res, id, 0, wait)
}

func (r *redisBackend) forceRelease(ctx context.Context, res string, wait int64) (bool, error) {
	return r.operate(ctx, "force", res, "", 0, wait)
}

func (r *redisBackend) exists(ctx context.Context, res, id string, wait int64) (bool, error) {
	return r.operate(ctx, "exists", res, id, 0, wait)
}

func (r *redisBackend) updateTTL(ctx context.Context, res, id string, ttl, wait int64) (bool, error) {
	return r.operate(ctx, "ttl", res, id, ttl, wait)
}

func (r *redisBackend) stop(context.Context) error {
	r.cancel()
	return r.client.Close()
}

func (r *redisBackend) requestContext(parent context.Context, wait int64) (context.Context, context.CancelFunc) {
	var ctx context.Context
	var cancel context.CancelFunc
	if wait == 0 {
		ctx, cancel = context.WithCancel(parent)
	} else {
		ctx, cancel = context.WithTimeout(parent, time.Duration(wait)*time.Microsecond)
	}
	stop := context.AfterFunc(r.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (r *redisBackend) operate(ctx context.Context, op, res, id string, ttl, wait int64) (bool, error) {
	ctx, cancel := r.requestContext(ctx, wait)
	defer cancel()
	ok, _, err := r.run(ctx, op, "rr:lock:"+res, id, ttl)
	return ok, err
}

func (r *redisBackend) run(ctx context.Context, op, key, id string, ttl int64) (bool, time.Duration, error) {
	values, err := r.script.Run(ctx, r.client, []string{key}, op, id, ttl).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return values[0] == 1, time.Duration(values[1]) * time.Microsecond, nil
}

func (r *redisBackend) acquire(ctx context.Context, op, res, id string, ttl, wait int64) (bool, error) {
	ctx, cancel := r.requestContext(ctx, wait)
	defer cancel()
	key := "rr:lock:" + res
	ok, delay, err := r.run(ctx, op, key, id, ttl)
	if err != nil || ok || delay == redisDuplicateReadDelay || wait <= 0 {
		return ok, acquisitionError(ctx, err)
	}

	sub := r.client.Subscribe(ctx, key)
	defer func() { _ = sub.Close() }()
	if _, err = sub.Receive(ctx); err != nil {
		return false, acquisitionError(ctx, err)
	}
	// Check lock state when the subscription reconnects.
	// https://redis.io/docs/latest/develop/pubsub/#delivery-semantics
	messages := sub.ChannelWithSubscriptions()
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()

	for {
		if err = ctx.Err(); err != nil {
			return false, acquisitionError(ctx, err)
		}
		// Check after subscription confirmation to cover a concurrent release.
		ok, delay, runErr := r.run(ctx, op, key, id, ttl)
		if runErr != nil || ok || delay == redisDuplicateReadDelay {
			return ok, acquisitionError(ctx, runErr)
		}
		var expiry <-chan time.Time
		if delay > 0 {
			timer.Reset(delay)
			expiry = timer.C
		}
		select {
		case <-ctx.Done():
			return false, acquisitionError(ctx, ctx.Err())
		case <-messages:
		case <-expiry:
		}
		timer.Stop()
	}
}

func acquisitionError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil
	}
	return err
}

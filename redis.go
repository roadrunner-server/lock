package lock

import (
	"context"
	_ "embed"
	"errors"
	"sync"
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

	// mu keeps the Pub/Sub commands in the order of the channel registry updates.
	mu       sync.Mutex
	sub      *redis.PubSub
	channels map[string]*channelWaiters
}

// channelWaiters holds the acquisitions that wait on one Pub/Sub channel.
type channelWaiters struct {
	// ready closes when Redis confirms the subscription.
	ready chan struct{}
	wakes map[chan struct{}]struct{}
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
	return &redisBackend{
		client:   client,
		script:   redis.NewScript(redisScript),
		ctx:      ctx,
		cancel:   cancel,
		channels: make(map[string]*channelWaiters),
	}, nil
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
	r.mu.Lock()
	sub := r.sub
	r.mu.Unlock()
	if sub != nil {
		_ = sub.Close()
	}
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
		return ok, err
	}

	wake := make(chan struct{}, 1)
	ready, err := r.addWaiter(key, wake)
	if err != nil {
		return false, err
	}
	defer r.removeWaiter(key, wake)
	// The loop maps an expired wait after this select.
	select {
	case <-ready:
	case <-ctx.Done():
	}
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()

	for {
		// The wait timeout is the only deadline on this context. Expiry here
		// means contention because no Redis command runs. Cancellation comes
		// from the plugin stop.
		if err = ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return false, nil
			}
			return false, err
		}
		// Check after subscription confirmation to cover a concurrent release.
		ok, delay, runErr := r.run(ctx, op, key, id, ttl)
		if runErr != nil || ok || delay == redisDuplicateReadDelay {
			return ok, runErr
		}
		var expiry <-chan time.Time
		if delay > 0 {
			timer.Reset(delay)
			expiry = timer.C
		}
		select {
		case <-ctx.Done():
		case <-wake:
		case <-expiry:
		}
		timer.Stop()
	}
}

// addWaiter registers wake for the channel and subscribes the shared connection on the first waiter.
// The returned channel closes when Redis confirms the subscription.
func (r *redisBackend) addWaiter(channel string, wake chan struct{}) (<-chan struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.sub == nil {
		r.sub = r.client.Subscribe(r.ctx)
		go r.dispatch(r.sub.ChannelWithSubscriptions())
	}
	if registered, subscribed := r.channels[channel]; subscribed {
		registered.wakes[wake] = struct{}{}
		return registered.ready, nil
	}
	waiters := &channelWaiters{
		ready: make(chan struct{}),
		wakes: map[chan struct{}]struct{}{wake: {}},
	}
	// The caller context can expire, so the backend context bounds the command.
	if err := r.sub.Subscribe(r.ctx, channel); err != nil {
		// A failed subscribe keeps the channel in the client subscription set.
		_ = r.sub.Unsubscribe(r.ctx, channel)
		return nil, err
	}
	r.channels[channel] = waiters
	return waiters.ready, nil
}

// removeWaiter drops wake and unsubscribes the shared connection when the last waiter leaves.
func (r *redisBackend) removeWaiter(channel string, wake chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiters, subscribed := r.channels[channel]
	if !subscribed {
		return
	}
	delete(waiters.wakes, wake)
	if len(waiters.wakes) > 0 {
		return
	}
	delete(r.channels, channel)
	_ = r.sub.Unsubscribe(r.ctx, channel)
}

// dispatch sends the messages of the shared connection to the waiters of each channel.
// It returns when stop closes the connection.
func (r *redisBackend) dispatch(messages <-chan any) {
	for message := range messages {
		switch message := message.(type) {
		case *redis.Message:
			r.wakeWaiters(message.Channel)
		case *redis.Subscription:
			// A reconnected subscription can miss messages.
			// https://redis.io/docs/latest/develop/pubsub/#delivery-semantics
			if message.Kind == "subscribe" && r.confirmSubscription(message.Channel) {
				r.wakeWaiters(message.Channel)
			}
		}
	}
}

// confirmSubscription marks the channel as active.
// It reports true when Redis confirms the channel again, which happens after a reconnect.
func (r *redisBackend) confirmSubscription(channel string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiters, subscribed := r.channels[channel]
	if !subscribed {
		return false
	}
	select {
	case <-waiters.ready:
		return true
	default:
		close(waiters.ready)
		return false
	}
}

// wakeWaiters signals every acquisition that waits on the channel.
func (r *redisBackend) wakeWaiters(channel string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiters, subscribed := r.channels[channel]
	if !subscribed {
		return
	}
	for wake := range waiters.wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

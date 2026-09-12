package lock

import (
	"context"
	_ "embed"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisDuplicateReadDelay is the Lua sentinel for terminal read refusal.
const redisDuplicateReadDelay = -2 * time.Microsecond

const redisSubscriptionHealthInterval = 3 * time.Second

//go:embed redis.lua
var redisScript string

type redisBackend struct {
	log    *slog.Logger
	client redis.UniversalClient
	script *redis.Script
	ctx    context.Context
	cancel context.CancelFunc

	// mu protects registry updates and the ordered subscription queue.
	mu         sync.Mutex
	sub        *redis.PubSub
	channels   map[string]*channelWaiters
	subscribed map[string]*channelWaiters
	changes    []*subscriptionChange
	pending    *subscriptionChange
	changed    chan struct{}
	receivers  sync.WaitGroup
	stopOnce   sync.Once
	stopped    chan struct{}
	stopErr    error
}

type subscriptionChange struct {
	sub       *redis.PubSub
	channel   string
	waiters   *channelWaiters
	subscribe bool
	done      chan struct{}
	retry     chan struct{}
	err       error
}

func (c *subscriptionChange) wait(ctx context.Context) error {
	for {
		select {
		case <-c.done:
			return c.err
		case <-ctx.Done():
			return ctx.Err()
		case <-c.retry:
			select {
			case <-c.done:
				return c.err
			default:
			}
			// UNSUBSCRIBE is idempotent. Its lost acknowledgment is absent after reconnect.
			if err := c.sub.Unsubscribe(ctx, c.channel); err != nil {
				return err
			}
		}
	}
}

// channelWaiters holds the acquisitions that wait on one Pub/Sub channel.
type channelWaiters struct {
	// ready closes when Redis confirms the subscription.
	ready chan struct{}
	wakes map[chan struct{}]struct{}

	// err is published by closing failed.
	failed chan struct{}
	err    error
}

func newRedisBackend(log *slog.Logger, cfg RedisConfig) (*redisBackend, error) {
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
	r := &redisBackend{
		log:        log,
		client:     client,
		script:     redis.NewScript(redisScript),
		ctx:        ctx,
		cancel:     cancel,
		channels:   make(map[string]*channelWaiters),
		subscribed: make(map[string]*channelWaiters),
		changed:    make(chan struct{}, 1),
		stopped:    make(chan struct{}),
	}
	hook := redisClientHook{ctx: ctx, connected: func() { r.retryUnsubscribe(nil) }}
	client.AddHook(hook)
	if cluster, ok := client.(*redis.ClusterClient); ok {
		cluster.OnNewNode(func(node *redis.Client) { node.AddHook(hook) })
	}
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		_ = client.Close()
		return nil, err
	}
	log.Info("lock backend initialized", "driver", "redis", "addrs", cfg.Addrs, "db", cfg.DB)
	r.receivers.Go(r.syncSubscriptions)
	return r, nil
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

func (r *redisBackend) stop(ctx context.Context) error {
	r.stopOnce.Do(func() {
		r.cancel()
		go func() {
			r.stopErr = r.client.Close()
			r.mu.Lock()
			sub := r.sub
			r.mu.Unlock()
			if sub != nil {
				_ = sub.Close()
			}
			r.receivers.Wait()
			r.mu.Lock()
			r.sub = nil
			r.changes = nil
			r.pending = nil
			clear(r.channels)
			clear(r.subscribed)
			r.mu.Unlock()
			close(r.stopped)
		}()
	})
	select {
	case <-r.stopped:
		return r.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
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
		// A request context that ends during the call reports the wait outcome, not a Redis failure.
		if ctx.Err() == nil {
			r.log.Error("redis lock script failed", "op", op, "key", key, "id", id, "error", err)
		}
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
	waiters, err := r.addWaiter(key, wake)
	if err != nil {
		return false, err
	}
	defer r.removeWaiter(key, waiters, wake)
	// The loop maps an expired wait after this select.
	select {
	case <-waiters.ready:
	case <-waiters.failed:
	case <-ctx.Done():
	}
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()

	for {
		select {
		case <-waiters.failed:
			return false, waiters.err
		default:
		}
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
		case <-waiters.failed:
		case <-wake:
		case <-expiry:
		}
		timer.Stop()
	}
}

// addWaiter registers wake and queues a subscription for the first waiter.
// The returned group reports subscription confirmation and receiver failure.
func (r *redisBackend) addWaiter(channel string, wake chan struct{}) (*channelWaiters, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.sub == nil {
		sub := r.client.Subscribe(r.ctx)
		r.sub = sub
		r.receivers.Go(func() { r.dispatch(sub) })
	}
	if registered, subscribed := r.channels[channel]; subscribed {
		registered.wakes[wake] = struct{}{}
		return registered, nil
	}
	waiters := &channelWaiters{
		ready:  make(chan struct{}),
		wakes:  map[chan struct{}]struct{}{wake: {}},
		failed: make(chan struct{}),
	}
	r.channels[channel] = waiters
	r.queueSubscription(channel, waiters, true)
	return waiters, nil
}

// removeWaiter drops wake and queues unsubscription when the last waiter leaves.
func (r *redisBackend) removeWaiter(channel string, waiters *channelWaiters, wake chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// A failed receiver can leave a replacement group on this channel.
	if r.channels[channel] != waiters {
		return
	}
	delete(waiters.wakes, wake)
	if len(waiters.wakes) > 0 {
		return
	}
	delete(r.channels, channel)
	r.queueSubscription(channel, waiters, false)
}

// queueSubscription is called with mu held. Socket I/O belongs to syncSubscriptions.
func (r *redisBackend) queueSubscription(channel string, waiters *channelWaiters, subscribe bool) {
	if r.ctx.Err() != nil {
		return
	}
	r.changes = append(r.changes, &subscriptionChange{
		sub: r.sub, channel: channel, waiters: waiters, subscribe: subscribe, done: make(chan struct{}),
		retry: make(chan struct{}, 1),
	})
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *redisBackend) syncSubscriptions() {
	for r.ctx.Err() == nil {
		r.mu.Lock()
		if len(r.changes) == 0 {
			r.mu.Unlock()
			select {
			case <-r.ctx.Done():
				return
			case <-r.changed:
			}
			continue
		}
		change := r.changes[0]
		r.changes[0] = nil
		r.changes = r.changes[1:]
		if change.sub != r.sub ||
			(change.subscribe && r.channels[change.channel] != change.waiters) ||
			(!change.subscribe && r.subscribed[change.channel] != change.waiters) {
			r.mu.Unlock()
			continue
		}
		r.pending = change
		if change.subscribe {
			r.subscribed[change.channel] = change.waiters
		}
		r.mu.Unlock()

		ctx, cancel := context.WithTimeout(r.ctx, 2*redisSubscriptionHealthInterval)
		var err error
		if change.subscribe {
			err = change.sub.Subscribe(ctx, change.channel)
		} else {
			err = change.sub.Unsubscribe(ctx, change.channel)
		}
		if err == nil {
			// Drain each confirmation before a new generation can use this channel.
			err = change.wait(ctx)
		}
		cancel()
		if err != nil {
			r.failSubscriptions(change.sub, err)
		}
	}
}

// dispatch receives notifications and server errors from one shared connection.
func (r *redisBackend) dispatch(sub *redis.PubSub) {
	ctx, cancel := context.WithCancel(r.ctx)
	var health sync.WaitGroup
	health.Go(func() { r.pingSubscription(ctx, sub) })
	defer func() {
		cancel()
		health.Wait()
	}()

	retry := time.NewTimer(0)
	retry.Stop()
	defer retry.Stop()
	for r.ctx.Err() == nil {
		message, err := r.receive(sub)
		if err != nil {
			if r.ctx.Err() != nil || errors.Is(err, redis.ErrClosed) {
				return
			}
			if _, serverError := errors.AsType[redis.Error](err); serverError {
				r.failSubscriptions(sub, err)
				return
			}
			// Receive reconnects after transport errors. Bound repeated dial failures.
			r.retryUnsubscribe(sub)
			retry.Reset(100 * time.Millisecond)
			select {
			case <-r.ctx.Done():
				return
			case <-retry.C:
			}
			continue
		}
		switch message := message.(type) {
		case *redis.Message:
			r.wakeWaiters(sub, message.Channel)
		case *redis.Subscription:
			// A reconnected subscription can miss messages.
			// https://redis.io/docs/latest/develop/pubsub/#delivery-semantics
			if r.confirmSubscription(sub, message) {
				r.retryUnsubscribe(sub)
				r.log.Warn("redis lock subscription reconnected", "channel", message.Channel)
				r.wakeWaiters(sub, message.Channel)
			}
		}
	}
}

// receive invalidates the connection if a bounded read cannot finish.
func (r *redisBackend) receive(sub *redis.PubSub) (any, error) {
	ctx, cancel := context.WithTimeout(r.ctx, 2*redisSubscriptionHealthInterval)
	defer cancel()
	return sub.Receive(ctx)
}

// pingSubscription checks health without interrupting an in-progress RESP read.
func (r *redisBackend) pingSubscription(ctx context.Context, sub *redis.PubSub) {
	ticker := time.NewTicker(redisSubscriptionHealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Ping reconnects on write errors; Receive consumes its reply.
			if err := sub.Ping(ctx); err != nil {
				r.retryUnsubscribe(sub)
			}
		}
	}
}

// retryUnsubscribe keeps the acknowledgment barrier after a physical connection replacement.
// A nil sub reports a dial before its connection is assigned to a PubSub or command pool.
func (r *redisBackend) retryUnsubscribe(sub *redis.PubSub) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if (sub == nil || r.sub == sub) && r.pending != nil && !r.pending.subscribe {
		select {
		case r.pending.retry <- struct{}{}:
		default:
		}
	}
}

// failSubscriptions reports a server error to every group on the failed connection.
// Redis error replies have no channel identifier.
func (r *redisBackend) failSubscriptions(sub *redis.PubSub, err error) {
	r.mu.Lock()
	if r.sub != sub {
		r.mu.Unlock()
		return
	}
	r.sub = nil
	failed := r.channels
	r.channels = make(map[string]*channelWaiters)
	clear(r.subscribed)
	if r.pending != nil {
		r.pending.err = err
		close(r.pending.done)
		r.pending = nil
	}
	r.mu.Unlock()
	for channel, waiters := range failed {
		r.log.Error("redis lock subscribe failed", "channel", channel, "error", err)
		waiters.err = err
		close(waiters.failed)
	}
	_ = sub.Close()
}

// confirmSubscription marks the channel as active.
// It reports true when Redis confirms the channel again, which happens after a reconnect.
func (r *redisBackend) confirmSubscription(sub *redis.PubSub, message *redis.Subscription) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sub != sub {
		return false
	}
	if change := r.pending; change != nil && change.channel == message.Channel &&
		((change.subscribe && message.Kind == "subscribe") || (!change.subscribe && message.Kind == "unsubscribe")) {
		if !change.subscribe {
			delete(r.subscribed, message.Channel)
		}
		close(change.done)
		r.pending = nil
	}
	waiters := r.subscribed[message.Channel]
	if message.Kind != "subscribe" || waiters == nil {
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
func (r *redisBackend) wakeWaiters(sub *redis.PubSub, channel string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sub != sub {
		return
	}
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

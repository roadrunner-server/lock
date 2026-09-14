package lock

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisSubscribeError(t *testing.T) {
	for _, tt := range []struct {
		name        string
		restriction string
		reconnect   bool
	}{
		{name: "command denied", restriction: "-subscribe"},
		{name: "channel denied", restriction: "resetchannels"},
		{name: "reconnect denied", restriction: "-subscribe", reconnect: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			admin := redisAdmin(t, 0)
			username := fmt.Sprintf("rr-lock-subscribe-%d", time.Now().UnixNano())
			require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username,
				"reset", "on", ">"+username, "+@all", "~*", "&*").Err())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				assert.NoError(t, admin.Do(ctx, "ACL", "DELUSER", username).Err())
			})

			holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
			cont, plugin, logs := observedLockContainer(t, &config.Plugin{
				Type: "yaml",
				ReadInCfg: fmt.Appendf(nil,
					"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q], username: %q, password: %q}}",
					redisAddr(), username, username),
			})
			waiter, _ := serveLockRPC(t, cont, plugin)
			clients, err := admin.ClientList(t.Context()).Result()
			require.NoError(t, err)
			require.Contains(t, clients, "user="+username+" ", "the RPC backend must authenticate as the restricted user")
			resource := t.Name()
			var held lockV1.Response
			require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
			require.True(t, held.GetOk())

			if !tt.reconnect {
				require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username, tt.restriction).Err())
			}
			const waiters = 5
			responses := make([]lockV1.Response, waiters)
			done := make(chan *rpc.Call, waiters)
			for i := range responses {
				waiter.Go("lock.Lock", &lockV1.Request{
					Resource: resource, Id: fmt.Sprintf("waiter-%d", i), Wait: new(int64(2_000_000)),
				}, &responses[i], done)
			}
			if tt.reconnect {
				waitForSubscriber(t, admin, resource)
				require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username, tt.restriction).Err())
				require.NoError(t, admin.ClientKillByFilter(t.Context(), "TYPE", "pubsub").Err())
			}
			for range responses {
				select {
				case call := <-done:
					require.ErrorContains(t, call.Error, "NOPERM", "subscription rejection must reach every waiting RPC")
					require.False(t, call.Reply.(*lockV1.Response).GetOk())
				case <-time.After(time.Second):
					t.Fatal("subscription rejection did not reach the waiting RPC")
				}
			}

			entries := logs.FilterMessage("redis lock subscribe failed").All()
			require.NotEmpty(t, entries, "a server-side subscription rejection must be logged")
			for _, entry := range entries {
				assert.Equal(t, slog.LevelError, entry.Level)
				assert.Equal(t, "rr:lock:"+resource, entry.Attrs["channel"])
				failure, ok := entry.Attrs["error"].(error)
				require.True(t, ok)
				assert.ErrorContains(t, failure, "NOPERM")
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				counts, err := admin.PubSubNumSub(t.Context(), "rr:lock:"+resource).Result()
				require.NoError(c, err)
				require.Zero(c, counts["rr:lock:"+resource])
			}, time.Second, 5*time.Millisecond)

			// The same backend must accept new waiters after permissions recover.
			require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username, "+subscribe", "&*").Err())
			var acquired lockV1.Response
			pending := waiter.Go("lock.Lock", &lockV1.Request{
				Resource: resource, Id: "recovered", Wait: new(int64(2_000_000)),
			}, &acquired, nil)
			waitForSubscriber(t, admin, resource)
			var released lockV1.Response
			require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
			require.True(t, released.GetOk())
			select {
			case call := <-pending.Done:
				require.NoError(t, call.Error)
				require.True(t, acquired.GetOk())
			case <-time.After(time.Second):
				t.Fatal("the shared subscription did not recover after rejection")
			}
		})
	}
}

func TestRedisWaitIdleSubscription(t *testing.T) {
	holder, waiter, admin := redisRPCClients(t)
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())
	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(10_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)
	clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
	require.NoError(t, err)
	fields := strings.Fields(clients)
	require.NotEmpty(t, fields)
	connection := fields[0]

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
		require.NoError(c, err)
		require.Contains(c, clients, connection+" ", "an idle healthy subscription keeps its connection")
		require.Contains(c, clients, "cmd=ping", "the shared connection checks its health while idle")
	}, 5*time.Second, 20*time.Millisecond)

	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())
	select {
	case call := <-pending.Done:
		require.NoError(t, call.Error)
		require.True(t, acquired.GetOk())
	case <-time.After(time.Second):
		t.Fatal("the idle subscription did not wake after release")
	}
}

func TestRedisWaitReconnectAfterLostPubSubReplies(t *testing.T) {
	admin := redisAdmin(t, 0)
	proxy, dropReplies := redisPubSubProxy(t, 0)
	holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	waiter, _ := lockRPCClient(t, &config.Plugin{
		Type: "yaml",
		ReadInCfg: fmt.Appendf(nil,
			"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
	})
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())
	before := redisScriptCalls(t, admin)
	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(12_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)
	// Both acquisition checks must observe the held lock before replies are lost.
	require.Eventually(t, func() bool { return redisScriptCalls(t, admin) >= before+2 }, time.Second, 5*time.Millisecond)
	dropReplies()
	// The old TCP connection stays open while its notifications and PONGs are lost.
	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())
	select {
	case call := <-pending.Done:
		require.NoError(t, call.Error)
		require.True(t, acquired.GetOk(), "reconnection must recheck the missed release")
	case <-time.After(9 * time.Second):
		t.Fatal("the subscriber did not recover after losing replies")
	}
}

func TestRedisWaitFragmentedNotification(t *testing.T) {
	admin := redisAdmin(t, 0)
	proxy, _ := redisPubSubProxy(t, 3500*time.Millisecond)
	holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	waiter, _ := lockRPCClient(t, &config.Plugin{
		Type: "yaml",
		ReadInCfg: fmt.Appendf(nil,
			"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
	})
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())
	before := redisScriptCalls(t, admin)
	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(8_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)
	// The notification must arrive after both acquisition checks observe contention.
	require.Eventually(t, func() bool { return redisScriptCalls(t, admin) >= before+2 }, time.Second, 5*time.Millisecond)
	clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
	require.NoError(t, err)
	fields := strings.Fields(clients)
	require.NotEmpty(t, fields)
	connection := fields[0]

	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())
	select {
	case call := <-pending.Done:
		require.NoError(t, call.Error)
		require.True(t, acquired.GetOk(), "a fragmented release notification must wake the waiting RPC")
	case <-time.After(9 * time.Second):
		t.Fatal("the fragmented release notification did not complete the waiting RPC")
	}
	clients, err = admin.ClientList(t.Context()).Result()
	require.NoError(t, err)
	require.Contains(t, clients, connection+" ", "the healthy subscription must survive a fragmented notification")
}

func redisScriptCalls(t *testing.T, admin *redis.Client) uint64 {
	t.Helper()
	stats, err := admin.Info(t.Context(), "commandstats").Result()
	require.NoError(t, err)
	for line := range strings.SplitSeq(stats, "\r\n") {
		if stats, found := strings.CutPrefix(line, "cmdstat_evalsha:calls="); found {
			calls, _, _ := strings.Cut(stats, ",")
			count, err := strconv.ParseUint(calls, 10, 64)
			require.NoError(t, err)
			return count
		}
	}
	t.Fatal("Redis did not report the acquisition script calls")
	return 0
}

func TestRedisWaitDeadlineDuringPubSubHandshake(t *testing.T) {
	admin := redisAdmin(t, 0)
	proxy, arm, stalled, _, resume := redisHandshakeProxy(t)
	holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	waiter, _ := lockRPCClient(t, &config.Plugin{
		Type: "yaml", ReadInCfg: fmt.Appendf(nil,
			"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
	})
	resource, other := t.Name(), t.Name()+"/other"
	t.Cleanup(func() { assert.NoError(t, admin.Del(context.Background(), "rr:lock:"+other).Err()) })
	resources := []struct {
		name     string
		resource string
	}{
		{name: "hold first resource", resource: resource},
		{name: "hold other resource", resource: other},
	}
	for _, tt := range resources {
		var held lockV1.Response
		require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: tt.resource, Id: "holder"}, &held), tt.name)
		require.True(t, held.GetOk(), tt.name)
	}
	arm()
	var first, second, survivor lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "short", Wait: new(int64(50_000)),
	}, &first, nil)
	select {
	case <-stalled:
	case <-time.After(time.Second):
		t.Fatal("the Pub/Sub handshake did not reach the proxy")
	}
	another := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: other, Id: "short", Wait: new(int64(50_000)),
	}, &second, nil)
	calls := []struct {
		name string
		call *rpc.Call
	}{
		{name: "first request deadline", call: pending},
		{name: "other request deadline", call: another},
	}
	for _, tt := range calls {
		select {
		case result := <-tt.call.Done:
			require.NoError(t, result.Error, tt.name)
			require.False(t, result.Reply.(*lockV1.Response).GetOk(), tt.name)
		case <-time.After(300 * time.Millisecond):
			t.Fatalf("%s: a stalled shared handshake bypassed an RPC wait deadline", tt.name)
		}
	}
	long := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "survivor", Wait: new(int64(2_000_000)),
	}, &survivor, nil)
	resume()
	waitForSubscriber(t, admin, resource)
	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())
	select {
	case result := <-long.Done:
		require.NoError(t, result.Error)
		require.True(t, survivor.GetOk(), "an expired caller must not cancel the shared receiver or its replacement group")
	case <-time.After(time.Second):
		t.Fatal("the replacement waiter did not recover after the handshake")
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		counts, err := admin.PubSubNumSub(t.Context(), "rr:lock:"+resource, "rr:lock:"+other).Result()
		require.NoError(c, err)
		require.Zero(c, counts["rr:lock:"+resource])
		require.Zero(c, counts["rr:lock:"+other])
	}, time.Second, time.Millisecond)
}

func TestRedisStopDuringPubSubHandshake(t *testing.T) {
	redisAdmin(t, 0)
	proxy, arm, stalled, closed, _ := redisHandshakeProxy(t)
	cont, plugin := lockContainer(t, &config.Plugin{
		Type: "yaml", ReadInCfg: fmt.Appendf(nil,
			"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
	})
	client, _ := serveLockRPC(t, cont, plugin)
	var held, response lockV1.Response
	require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: t.Name(), Id: "holder"}, &held))
	require.True(t, held.GetOk())
	arm()
	pending := client.Go("lock.Lock", &lockV1.Request{
		Resource: t.Name(), Id: "waiter", Wait: new(int64(10_000_000)),
	}, &response, nil)
	select {
	case <-stalled:
	case <-time.After(time.Second):
		t.Fatal("the Pub/Sub handshake did not reach the proxy")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- plugin.Stop(ctx) }()
	select {
	case err := <-stopped:
		if err != nil {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("stop bypassed its deadline during Pub/Sub initialization")
	}
	select {
	case <-closed:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("stop did not close the initializing Pub/Sub socket")
	}
	select {
	case result := <-pending.Done:
		require.Error(t, result.Error)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("stop did not cancel the pending RPC")
	}
}

// redisHandshakeProxy holds the first reply on the next connection after arm.
func redisHandshakeProxy(t *testing.T) (string, func(), <-chan struct{}, <-chan struct{}, func()) {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var armed atomic.Bool
	stalled, closed, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	var workers sync.WaitGroup
	ctx := t.Context()
	workers.Go(func() {
		for {
			caller, err := listener.Accept()
			if err != nil {
				return
			}
			stall := armed.CompareAndSwap(true, false)
			workers.Go(func() {
				defer func() { _ = caller.Close() }()
				var dialer net.Dialer
				server, err := dialer.DialContext(ctx, "tcp", redisAddr())
				if err != nil {
					return
				}
				defer func() { _ = server.Close() }()
				stop := context.AfterFunc(ctx, func() {
					_ = caller.Close()
					_ = server.Close()
				})
				defer stop()
				workers.Go(func() {
					_, _ = io.Copy(server, caller)
					_ = server.Close()
					if stall {
						close(closed)
					}
				})
				if stall {
					frame, err := readRedisFrame(bufio.NewReader(server))
					if err != nil {
						return
					}
					close(stalled)
					select {
					case <-ctx.Done():
						return
					case <-closed:
						return
					case <-release:
					}
					if _, err = caller.Write(frame); err != nil {
						return
					}
				}
				_, _ = io.Copy(caller, server)
			})
		}
	})
	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
		workers.Wait()
	})
	return listener.Addr().String(), func() { armed.Store(true) }, stalled, closed, resume
}

// redisPubSubProxy can drop replies or pause notifications after the message field.
func redisPubSubProxy(t *testing.T, fragmentDelay time.Duration) (string, func()) {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	type connection struct {
		pubsub  atomic.Bool
		blocked atomic.Bool
	}
	var mu sync.Mutex
	connections := make(map[*connection]struct{})
	var workers sync.WaitGroup
	ctx := t.Context()
	workers.Go(func() {
		for {
			caller, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer func() { _ = caller.Close() }()
				var dialer net.Dialer
				server, err := dialer.DialContext(ctx, "tcp", redisAddr())
				if err != nil {
					return
				}
				defer func() { _ = server.Close() }()
				stop := context.AfterFunc(ctx, func() {
					_ = caller.Close()
					_ = server.Close()
				})
				defer stop()
				current := &connection{}
				mu.Lock()
				connections[current] = struct{}{}
				mu.Unlock()
				defer func() {
					mu.Lock()
					delete(connections, current)
					mu.Unlock()
				}()
				workers.Go(func() {
					_, _ = io.Copy(server, caller)
					_ = server.Close()
				})
				reader := bufio.NewReader(server)
				for {
					line, err := reader.ReadBytes('\n')
					if strings.EqualFold(string(line), "subscribe\r\n") {
						current.pubsub.Store(true)
					}
					if len(line) > 0 && !current.blocked.Load() {
						if _, writeErr := caller.Write(line); writeErr != nil {
							return
						}
						if fragmentDelay > 0 && current.pubsub.Load() && string(line) == "message\r\n" {
							timer := time.NewTimer(fragmentDelay)
							select {
							case <-ctx.Done():
								timer.Stop()
								return
							case <-timer.C:
							}
						}
					}
					if err != nil {
						return
					}
				}
			})
		}
	})
	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
		workers.Wait()
	})
	return listener.Addr().String(), func() {
		t.Helper()
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			for current := range connections {
				if current.pubsub.Load() {
					current.blocked.Store(true)
					return true
				}
			}
			return false
		}, time.Second, time.Millisecond)
	}
}

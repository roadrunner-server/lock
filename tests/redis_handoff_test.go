package lock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisUnsubscribeSilentHandoff(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement=%t", replacement), func(t *testing.T) {
			admin := redisAdmin(t, 0)
			expiring, queuedResource := t.Name(), t.Name()+"/new"
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				assert.NoError(t, admin.Del(ctx, "rr:lock:"+queuedResource).Err())
			})
			if replacement {
				queuedResource = expiring
			}
			proxy, pongHeld, handedOff, contended := redisSilentHandoffProxy(t, "rr:lock:"+expiring, "rr:lock:"+queuedResource)
			holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
			cont, plugin, logs := observedLockContainer(t, &config.Plugin{
				Type: "yaml", ReadInCfg: fmt.Appendf(nil,
					"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
			})
			waiter, _ := serveLockRPC(t, cont, plugin)
			resources := []string{expiring}
			if !replacement {
				resources = append(resources, queuedResource)
			}
			for _, resource := range resources {
				var held lockV1.Response
				require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
				require.True(t, held.GetOk())
			}
			var expired lockV1.Response
			short := waiter.Go("lock.Lock", &lockV1.Request{
				Resource: expiring, Id: "expiring", Wait: new(int64(4_000_000)),
			}, &expired, nil)
			waitForSubscriber(t, admin, expiring)
			select {
			case <-pongHeld:
			case <-time.After(3500 * time.Millisecond):
				t.Fatal("the proxy did not hold the outstanding health PONG")
			}
			select {
			case <-handedOff:
			case <-time.After(4 * time.Second):
				t.Fatal("MOVING did not establish a healthy connection to its target endpoint")
			}
			require.NoError(t, (<-short.Done).Error)
			require.False(t, expired.GetOk())
			counts, err := admin.PubSubNumSub(t.Context(), "rr:lock:"+expiring).Result()
			require.NoError(t, err)
			require.Zero(t, counts["rr:lock:"+expiring], "the last channel must be absent after handoff")

			var acquired lockV1.Response
			queued := waiter.Go("lock.Lock", &lockV1.Request{
				Resource: queuedResource, Id: "queued", Wait: new(int64(1_000_000)),
			}, &acquired, nil)
			select {
			case <-contended:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("the queued RPC did not observe real Redis contention")
			}
			var released lockV1.Response
			require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: queuedResource, Id: "holder"}, &released))
			require.True(t, released.GetOk())
			select {
			case result := <-queued.Done:
				assert.NoError(t, result.Error)
				assert.True(t, acquired.GetOk(), "a successful-I/O handoff must recover the pending last-channel unsubscribe")
			case <-time.After(1500 * time.Millisecond):
				t.Fatal("the queued RPC exceeded its wait after silent handoff")
			}
			assert.Empty(t, logs.FilterMessage("redis lock subscribe failed").All())
			assert.Empty(t, logs.FilterMessage("redis lock subscription reconnected").All(), "the handoff has no surviving subscription confirmation")
		})
	}
}

// redisSilentHandoffProxy enables maintenance notifications and injects MOVING before
// a held real PONG and the last-channel UNSUBSCRIBE reply. The client closes the old stream.
func redisSilentHandoffProxy(t *testing.T, expiringChannel, queuedChannel string) (string, <-chan struct{}, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	var listen net.ListenConfig
	listeners := make([]net.Listener, 2)
	for i := range listeners {
		listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = listener
		t.Cleanup(func() { _ = listener.Close() })
	}
	pongHeld, handedOff, contended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	healthy := sync.OnceFunc(func() { close(handedOff) })
	contention := sync.OnceFunc(func() { close(contended) })
	endpoint := listeners[1].Addr().String()
	moving := fmt.Appendf(nil, ">4\r\n$6\r\nMOVING\r\n:1\r\n:60\r\n$%d\r\n%s\r\n", len(endpoint), endpoint)
	unsubscribe := fmt.Appendf(nil, "$11\r\nunsubscribe\r\n$%d\r\n%s\r\n", len(expiringChannel), expiringChannel)
	var workers sync.WaitGroup
	ctx := t.Context()
	for i, listener := range listeners {
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
					requests, replies := bufio.NewReader(caller), bufio.NewReader(server)
					for {
						wire, err := readRedisFrame(requests)
						if err != nil {
							return
						}
						args, err := redisCommandArgs(wire)
						if err != nil || len(args) == 0 {
							return
						}
						if args[0] == "client" && args[1] == "maint_notifications" {
							if _, err = io.WriteString(caller, "+OK\r\n"); err != nil {
								return
							}
							continue
						}
						if _, err = server.Write(wire); err != nil {
							return
						}
						if args[0] == "subscribe" || (i == 1 && (args[0] == "ping" || args[0] == "unsubscribe")) {
							break
						}
						reply, err := readRedisFrame(replies)
						if err != nil {
							return
						}
						if len(args) > 5 && (args[0] == "evalsha" || args[0] == "eval") &&
							args[3] == queuedChannel && args[4] == "lock" && args[5] == "queued" &&
							bytes.HasPrefix(reply, []byte("*2\r\n:0\r\n")) {
							contention()
						}
						if _, err = caller.Write(reply); err != nil {
							return
						}
					}
					workers.Go(func() {
						_, _ = io.Copy(server, requests)
						_ = server.Close()
					})
					var pong []byte
					for {
						frame, err := readRedisFrame(replies)
						if err != nil {
							return
						}
						isPong := bytes.Equal(frame, []byte("+PONG\r\n"))
						if i == 0 && isPong && pong == nil {
							pong = frame
							close(pongHeld)
							continue
						}
						if i == 0 && bytes.Contains(frame, unsubscribe) && pong != nil {
							frame = append(append(bytes.Clone(moving), pong...), frame...)
						}
						if _, err = caller.Write(frame); err != nil {
							return
						}
						if i == 1 && isPong {
							healthy()
						}
					}
				})
			}
		})
	}
	t.Cleanup(func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
		workers.Wait()
	})
	return listeners[0].Addr().String(), pongHeld, handedOff, contended
}

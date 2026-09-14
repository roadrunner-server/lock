package lock

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"sync"
	"testing"
	"time"

	mocklogger "tests/mock"

	"github.com/redis/go-redis/v9"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	goridgeRPC "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	lockPlugin "github.com/roadrunner-server/lock/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lockContainer(t *testing.T, cfg *config.Plugin) (*endure.Endure, *lockPlugin.Plugin) {
	t.Helper()
	cont := endure.New(slog.LevelError)
	plugin := &lockPlugin.Plugin{}
	require.NoError(t, cont.RegisterAll(cfg, &logger.Plugin{}, plugin))
	return cont, plugin
}

func observedLockContainer(t *testing.T, cfg *config.Plugin) (*endure.Endure, *lockPlugin.Plugin, *mocklogger.ObservedLogs) {
	t.Helper()
	cont := endure.New(slog.LevelError)
	plugin := &lockPlugin.Plugin{}
	log, logs := mocklogger.SlogTestLogger(slog.LevelDebug)
	require.NoError(t, cont.RegisterAll(cfg, log, plugin))
	return cont, plugin, logs
}

func lockRPCClient(t *testing.T, cfg *config.Plugin) (*rpc.Client, func() error) {
	t.Helper()
	cont, plugin := lockContainer(t, cfg)
	return serveLockRPC(t, cont, plugin)
}

func serveLockRPC(t *testing.T, cont *endure.Endure, plugin *lockPlugin.Plugin) (*rpc.Client, func() error) {
	t.Helper()
	require.NoError(t, cont.Init())
	stop := sync.OnceValue(cont.Stop)

	server := rpc.NewServer()
	require.NoError(t, server.RegisterName(plugin.Name(), plugin.RPC()))
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		server.ServeCodec(goridgeRPC.NewCodec(serverConn))
		close(done)
	}()
	client := rpc.NewClientWithCodec(goridgeRPC.NewClientCodec(clientConn))
	t.Cleanup(func() {
		assert.NoError(t, stop())
		assert.NoError(t, client.Close())
		<-done
	})
	return client, stop
}

func redisRPCClients(t *testing.T) (*rpc.Client, *rpc.Client, *redis.Client) {
	t.Helper()
	admin := redisAdmin(t, 0)
	first, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	second, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	return first, second, admin
}

func redisAddr() string {
	if addr := os.Getenv("RR_LOCK_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:16379"
}

func redisAdmin(t *testing.T, db int) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: redisAddr(), DB: db, MaxRetries: -1, ContextTimeoutEnabled: true})
	key := "rr:lock:" + t.Name()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoError(t, client.Del(ctx, key).Err())
		assert.NoError(t, client.Close())
	})
	require.NoError(t, client.Del(t.Context(), key).Err())
	return client
}

// slowRedis returns the address of a proxy to the test Redis. The proxy sends
// each command immediately and holds each reply for the given delay.
func slowRedis(t *testing.T, delay time.Duration) string {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, listener.Close()) })
	ctx := t.Context()
	go func() {
		for {
			caller, err := listener.Accept()
			if err != nil {
				return
			}
			go delayReplies(ctx, caller, redisAddr(), delay)
		}
	}()
	return listener.Addr().String()
}

func delayReplies(ctx context.Context, caller net.Conn, addr string, delay time.Duration) {
	defer func() { _ = caller.Close() }()
	var dialer net.Dialer
	server, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	go func() {
		_, _ = io.Copy(server, caller)
		_ = server.Close()
	}()
	buffer := make([]byte, 4096)
	for {
		n, err := server.Read(buffer)
		if n > 0 {
			time.Sleep(delay)
			if _, werr := caller.Write(buffer[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func waitForSubscriber(t *testing.T, admin *redis.Client, resource string) {
	t.Helper()
	channel := "rr:lock:" + resource
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		counts, err := admin.PubSubNumSub(t.Context(), channel).Result()
		require.NoError(c, err)
		require.EqualValues(c, 1, counts[channel])
	}, time.Second, time.Millisecond)
}

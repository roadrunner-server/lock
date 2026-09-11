package lock

import (
	"context"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"sync"
	"testing"
	"time"

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

func lockRPCClient(t *testing.T, cfg *config.Plugin) (*rpc.Client, func() error) {
	t.Helper()
	cont, plugin := lockContainer(t, cfg)
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

func redisAdmin(t *testing.T, db int) *redis.Client {
	t.Helper()
	addr := os.Getenv("RR_LOCK_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:16379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: db, MaxRetries: -1, ContextTimeoutEnabled: true})
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

func waitForSubscriber(t *testing.T, admin *redis.Client, resource string) {
	t.Helper()
	channel := "rr:lock:" + resource
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		counts, err := admin.PubSubNumSub(t.Context(), channel).Result()
		require.NoError(c, err)
		require.EqualValues(c, 1, counts[channel])
	}, time.Second, time.Millisecond)
}

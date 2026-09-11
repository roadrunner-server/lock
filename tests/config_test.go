package lock

import (
	"net/rpc"
	"testing"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryDefault(t *testing.T) {
	first, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-init.yaml", Flags: []string{"logs.level=error"}})
	second, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-init.yaml", Flags: []string{"logs.level=error"}})
	for _, client := range []*rpc.Client{first, second} {
		var response lockV1.Response
		require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: t.Name(), Id: "owner"}, &response))
		assert.True(t, response.GetOk(), "default locks must be local to each RR instance")
	}
}

func TestMemoryExplicitDriver(t *testing.T) {
	memoryConfig := func() *config.Plugin {
		return &config.Plugin{Type: "yaml", ReadInCfg: []byte("version: '3'\nlogs: {level: error}\nlock: {driver: memory}")}
	}
	first, _ := lockRPCClient(t, memoryConfig())
	second, _ := lockRPCClient(t, memoryConfig())
	for _, client := range []*rpc.Client{first, second} {
		var response lockV1.Response
		require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: t.Name(), Id: "owner"}, &response))
		assert.True(t, response.GetOk(), "memory locks must be local to each RR instance")
	}
}

func TestRedisInvalidConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name    string
		section string
		message string
	}{
		{name: "missing driver", section: "lock: {}"},
		{name: "unknown driver", section: "lock: {driver: unknown}"},
		{name: "invalid database", section: "lock: {driver: redis, config: {db: invalid}}"},
		{name: "empty addresses", section: "lock: {driver: redis, config: {addrs: []}}"},
		{
			name:    "database with cluster addresses",
			section: "lock: {driver: redis, config: {addrs: ['127.0.0.1:1', '127.0.0.1:2'], db: 1}}",
			message: "cluster client uses database 0",
		},
		{
			name:    "negative dial timeout",
			section: "lock: {driver: redis, config: {addrs: ['127.0.0.1:1'], dial_timeout: -1s}}",
			message: "dial_timeout must not be negative",
		},
		{
			name:    "negative read timeout",
			section: "lock: {driver: redis, config: {addrs: ['127.0.0.1:1'], read_timeout: -1}}",
			message: "read_timeout must not be negative",
		},
		{
			name:    "negative write timeout",
			section: "lock: {driver: redis, config: {addrs: ['127.0.0.1:1'], write_timeout: -1s}}",
			message: "write_timeout must not be negative",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cont, _ := lockContainer(t, &config.Plugin{
				Type: "yaml", ReadInCfg: []byte("version: '3'\nlogs: {level: error}\n" + tt.section),
			})
			err := cont.Init()
			if err == nil {
				t.Cleanup(func() { assert.NoError(t, cont.Stop()) })
			}
			require.Error(t, err)
			if tt.message != "" {
				assert.ErrorContains(t, err, tt.message)
			}
		})
	}
}

func TestRedisInitializationError(t *testing.T) {
	for _, tt := range []struct {
		name  string
		flags []string
	}{
		{name: "connection refused", flags: []string{"lock.config.addrs=127.0.0.1:1", "lock.config.dial_timeout=50ms"}},
		{name: "authentication rejected", flags: []string{"lock.config.username=rr-lock-missing", "lock.config.password=incorrect"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cont, _ := lockContainer(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml", Flags: tt.flags})
			err := cont.Init()
			if err == nil {
				t.Cleanup(func() { assert.NoError(t, cont.Stop()) })
			}
			require.Error(t, err)
		})
	}
}

func TestRedisDatabaseSelection(t *testing.T) {
	redisAdmin(t, 1)
	databaseOne := &config.Plugin{Path: "configs/.rr-lock-redis.yaml", Flags: []string{"lock.config.db=1"}}
	holder, _ := lockRPCClient(t, databaseOne)
	observer, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml", Flags: []string{"lock.config.db=1"}})
	otherDatabase, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	resource := t.Name()

	var acquired lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner"}, &acquired))
	require.True(t, acquired.GetOk())

	var sameDatabase lockV1.Response
	require.NoError(t, observer.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &sameDatabase))
	assert.True(t, sameDatabase.GetOk())

	var differentDatabase lockV1.Response
	require.NoError(t, otherDatabase.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &differentDatabase))
	assert.False(t, differentDatabase.GetOk())
}

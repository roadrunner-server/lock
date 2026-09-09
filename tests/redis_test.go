package lock

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	lockPlugin "github.com/roadrunner-server/lock/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lockService interface {
	Lock(*lockV1.Request, *lockV1.Response) error
	LockRead(*lockV1.Request, *lockV1.Response) error
	Release(*lockV1.Request, *lockV1.Response) error
	ForceRelease(*lockV1.Request, *lockV1.Response) error
	Exists(*lockV1.Request, *lockV1.Response) error
	UpdateTTL(*lockV1.Request, *lockV1.Response) error
}

type lockMethod func(*lockV1.Request, *lockV1.Response) error

func TestMemoryDefault(t *testing.T) {
	a, _ := newBackend(t, "")
	b, _ := newBackend(t, "")
	checkLock(t, a.Lock, t.Name(), "A", 0, 0, true)
	checkLock(t, b.Lock, t.Name(), "B", 0, 0, true)
}

func TestRedisConfig(t *testing.T) {
	for name, section := range map[string]string{
		"missing driver": "lock: {}",
		"unknown driver": "lock: {driver: unknown}",
		"invalid db":     "lock: {driver: redis, config: {db: invalid}}",
		"empty addrs":    "lock: {driver: redis, config: {addrs: []}}",
		"connection":     "lock: {driver: redis, config: {addrs: ['127.0.0.1:1'], dial_timeout: 50ms}}",
		"authentication": fmt.Sprintf("lock: {driver: redis, config: {addrs: [%q], username: rr-lock-missing, password: incorrect}}", redisAddress()),
	} {
		t.Run(name, func(t *testing.T) {
			cont, _ := backendContainer(t, section)
			err := cont.Init()
			if err == nil {
				require.NoError(t, cont.Stop())
			}
			require.Error(t, err)
		})
	}
}

func TestRedisExclusive(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))

	checkLock(t, a.Lock, res, "A", 0, 0, true)
	checkLock(t, b.Lock, res, "B", 0, 0, false)
	checkLock(t, b.LockRead, res, "B", 0, 0, false)
	checkLock(t, b.Lock, res, "A", 0, 0, false)
	checkLock(t, b.Exists, res, "A", 0, 0, true)
	checkLock(t, b.Exists, res, "*", 0, 0, true)
	checkLock(t, b.Release, res, "B", 0, 0, false)
	checkLock(t, b.UpdateTTL, res, "B", 100, 0, false)
	checkLock(t, b.Release, res, "A", 0, 0, true)
	checkLock(t, a.Lock, res, "B", 0, 0, true)
}

func TestRedisReadersAndPromotion(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))

	checkLock(t, a.LockRead, res, "A", 0, 0, true)
	checkLock(t, b.LockRead, res, "B", 0, 0, true)
	checkLock(t, a.Exists, res, "B", 0, 0, true)
	checkLock(t, a.Lock, res, "A", 0, 0, false)
	checkLock(t, a.Release, res, "B", 0, 0, true)
	checkLock(t, b.Lock, res, "A", 0, 0, true)
	checkLock(t, a.LockRead, res, "C", 0, 0, false)
	checkLock(t, a.Release, res, "A", 0, 0, true)
	checkLock(t, b.LockRead, res, "C", 0, 0, true)
}

func TestRedisConcurrentWriters(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))
	services := []lockService{a, b}
	start := make(chan struct{})
	results := make(chan acquireResult, 20)
	for i := range 20 {
		go func() {
			<-start
			var out lockV1.Response
			err := services[i%2].Lock(&lockV1.Request{Resource: res, Id: fmt.Sprint(i)}, &out)
			results <- acquireResult{ok: out.GetOk(), err: err}
		}()
	}
	close(start)
	winners := 0
	for range 20 {
		got := <-results
		require.NoError(t, got.err)
		if got.ok {
			winners++
		}
	}
	require.Equal(t, 1, winners)
}

func TestRedisForceRelease(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))

	checkLock(t, a.LockRead, res, "A", 0, 0, true)
	checkLock(t, b.LockRead, res, "B", 0, 0, true)
	checkLock(t, b.ForceRelease, res, "", 0, 0, true)
	checkLock(t, a.Exists, res, "*", 0, 0, false)
	checkLock(t, b.Exists, res, "B", 0, 0, false)
	checkLock(t, a.Lock, res, "C", 0, 0, true)
}

func TestRedisReaderExpiry(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))

	checkLock(t, a.LockRead, res, "A", 100_000, 0, true)
	checkLock(t, b.LockRead, res, "B", 0, 0, true)
	waitLockAbsent(t, b, res, "A")
	checkLock(t, a.Exists, res, "B", 0, 0, true)
	checkLock(t, b.UpdateTTL, res, "A", 1_000_000, 0, false)
	checkLock(t, a.Lock, res, "C", 0, 0, false)
	checkLock(t, a.UpdateTTL, res, "B", 50_000, 0, true)
	checkLock(t, b.Lock, res, "C", 0, 2_000_000, true)
	checkLock(t, a.Release, res, "B", 0, 0, false)
	checkLock(t, a.Exists, res, "C", 0, 0, true)
}

func TestRedisUpdatePersistentTTL(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))

	checkLock(t, a.Lock, res, "A", 100_000, 0, true)
	checkLock(t, b.UpdateTTL, res, "A", 0, 0, true)
	time.Sleep(200 * time.Millisecond)
	checkLock(t, b.Exists, res, "A", 0, 0, true)
	checkLock(t, b.UpdateTTL, res, "A", 50_000, 0, true)
	waitLockAbsent(t, a, res, "A")
	checkLock(t, b.UpdateTTL, res, "A", 0, 0, false)
}

func TestRedisSubMillisecondTTL(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))
	checkLock(t, a.Lock, res, "A", 100, 0, true)
	waitLockAbsent(t, b, res, "A")
	checkLock(t, b.Lock, res, "B", 0, 0, true)
}

func TestRedisWaitExpiry(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))

	checkLock(t, a.Lock, res, "A", 150_000, 0, true)
	checkLock(t, b.LockRead, res, "B", 0, 2_000_000, true)
	checkLock(t, a.Exists, res, "A", 0, 0, false)
	checkLock(t, a.Exists, res, "B", 0, 0, true)
}

func TestRedisWaitNotifications(t *testing.T) {
	for _, action := range []string{"release", "force release", "update ttl"} {
		t.Run(action, func(t *testing.T) {
			client, res := redisResource(t, 0)
			a, _ := newBackend(t, redisSection(0))
			b, _ := newBackend(t, redisSection(0))
			checkLock(t, a.Lock, res, "A", 0, 0, true)
			result := acquireAsync(b.Lock, res)
			waitSubscriber(t, client, res)

			switch action {
			case "release":
				checkLock(t, a.Release, res, "A", 0, 0, true)
			case "force release":
				checkLock(t, a.ForceRelease, res, "", 0, 0, true)
			case "update ttl":
				checkLock(t, a.UpdateTTL, res, "A", 20_000, 0, true)
			}
			requireAcquired(t, result)
		})
	}
}

func TestRedisWaitTimeout(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))
	checkLock(t, a.Lock, res, "A", 0, 0, true)
	start := time.Now()
	checkLock(t, b.Lock, res, "B", 0, 80_000, false)
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)
	checkLock(t, b.Exists, res, "B", 0, 0, false)
}

func TestRedisWaitReconnect(t *testing.T) {
	client, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))
	checkLock(t, a.Lock, res, "A", 0, 0, true)
	result := acquireAsync(b.Lock, res)
	waitSubscriber(t, client, res)

	// Release while the subscription is disconnected, so no notification reaches it.
	_, err := client.TxPipelined(t.Context(), func(pipe redis.Pipeliner) error {
		pipe.ClientKillByFilter(t.Context(), "TYPE", "pubsub")
		pipe.Del(t.Context(), "rr:lock:"+res)
		return nil
	})
	require.NoError(t, err)
	requireAcquired(t, result)
}

func TestRedisZeroWaitNetworkTimeout(t *testing.T) {
	client, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	require.NoError(t, client.ClientPause(t.Context(), 30*time.Millisecond).Err())
	checkLock(t, a.Lock, res, "A", 0, 0, true)
	checkLock(t, a.Release, res, "A", 0, 0, true)
}

func TestRedisStopWaiter(t *testing.T) {
	client, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	b, stop := newBackend(t, redisSection(0))
	checkLock(t, a.Lock, res, "A", 0, 0, true)
	result := acquireAsync(b.Lock, res)
	waitSubscriber(t, client, res)
	stop()
	select {
	case got := <-result:
		require.False(t, got.ok)
		require.Error(t, got.err)
	case <-time.After(2 * time.Second):
		t.Fatal("lock call did not stop")
	}
	checkLock(t, a.Exists, res, "A", 0, 0, true)
}

func TestRedisLocksSurviveStop(t *testing.T) {
	_, res := redisResource(t, 0)
	a, stop := newBackend(t, redisSection(0))
	b, _ := newBackend(t, redisSection(0))
	checkLock(t, a.Lock, res, "A", 0, 0, true)
	stop()
	checkLock(t, b.Exists, res, "A", 0, 0, true)
	checkLock(t, b.Release, res, "A", 0, 0, true)
}

func TestRedisDatabase(t *testing.T) {
	client, res := redisResource(t, 1)
	a, _ := newBackend(t, redisSection(1))
	b, _ := newBackend(t, redisSection(0))
	checkLock(t, a.Lock, res, "A", 0, 0, true)
	checkLock(t, b.Exists, res, "A", 0, 0, false)
	require.EqualValues(t, 1, client.Exists(t.Context(), "rr:lock:"+res).Val())
}

func TestRedisScriptError(t *testing.T) {
	client, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	require.NoError(t, client.Set(t.Context(), "rr:lock:"+res, "wrong type", 0).Err())
	var out lockV1.Response
	err := a.Exists(&lockV1.Request{Resource: res, Id: "*"}, &out)
	require.Error(t, err)
	require.False(t, out.GetOk())
}

func TestRedisEmptyID(t *testing.T) {
	_, res := redisResource(t, 0)
	a, _ := newBackend(t, redisSection(0))
	for _, method := range []lockMethod{a.Lock, a.LockRead, a.Release, a.Exists, a.UpdateTTL} {
		var out lockV1.Response
		require.Error(t, method(&lockV1.Request{Resource: res}, &out))
	}
}

func backendContainer(t *testing.T, section string) (*endure.Endure, *lockPlugin.Plugin) {
	t.Helper()
	cont := endure.New(slog.LevelError)
	p := &lockPlugin.Plugin{}
	require.NoError(t, cont.RegisterAll(
		&config.Plugin{Type: "yaml", ReadInCfg: []byte("version: '3'\nlogs: {level: error}\n" + section)},
		&logger.Plugin{},
		p,
	))
	return cont, p
}

func newBackend(t *testing.T, section string) (lockService, func()) {
	t.Helper()
	cont, p := backendContainer(t, section)
	require.NoError(t, cont.Init())
	stop := sync.OnceFunc(func() { require.NoError(t, cont.Stop()) })
	t.Cleanup(stop)
	return p.RPC().(lockService), stop
}

func redisAddress() string {
	if addr := os.Getenv("RR_LOCK_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:16379"
}

func redisSection(db int) string {
	return fmt.Sprintf("lock:\n  driver: redis\n  config:\n    addrs: [%q]\n    db: %d\n", redisAddress(), db)
}

func redisResource(t *testing.T, db int) (*redis.Client, string) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: redisAddress(), DB: db})
	require.NoError(t, client.Ping(t.Context()).Err())
	res := t.Name() + ":" + rand.Text()
	t.Cleanup(func() {
		require.NoError(t, client.Del(context.Background(), "rr:lock:"+res).Err())
		require.NoError(t, client.Close())
	})
	return client, res
}

func checkLock(t *testing.T, method lockMethod, res, id string, ttl, wait int64, want bool) {
	t.Helper()
	var out lockV1.Response
	err := method(&lockV1.Request{Resource: res, Id: id, Ttl: new(ttl), Wait: new(wait)}, &out)
	require.NoError(t, err)
	require.Equal(t, want, out.GetOk(), "resource %s, id %s", res, id)
}

func waitLockAbsent(t *testing.T, service lockService, res, id string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var out lockV1.Response
		require.NoError(c, service.Exists(&lockV1.Request{Resource: res, Id: id}, &out))
		require.False(c, out.GetOk())
	}, 2*time.Second, 5*time.Millisecond)
}

type acquireResult struct {
	ok  bool
	err error
}

func acquireAsync(method lockMethod, res string) <-chan acquireResult {
	ch := make(chan acquireResult, 1)
	go func() {
		var out lockV1.Response
		err := method(&lockV1.Request{Resource: res, Id: "B", Wait: new(int64(5_000_000))}, &out)
		ch <- acquireResult{ok: out.GetOk(), err: err}
	}()
	return ch
}

func waitSubscriber(t *testing.T, client *redis.Client, res string) {
	t.Helper()
	channel := "rr:lock:" + res
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		counts, err := client.PubSubNumSub(t.Context(), channel).Result()
		require.NoError(c, err)
		require.Positive(c, counts[channel])
	}, time.Second, time.Millisecond)
}

func requireAcquired(t *testing.T, result <-chan acquireResult) {
	t.Helper()
	select {
	case got := <-result:
		require.NoError(t, got.err)
		require.True(t, got.ok)
	case <-time.After(2 * time.Second):
		t.Fatal("lock call did not receive a notification")
	}
}

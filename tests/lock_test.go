package lock

import (
	"crypto/rand"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	mocklogger "tests/mock"

	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	lockPlugin "github.com/roadrunner-server/lock/v6"
	"github.com/roadrunner-server/logger/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const secMult = 1000000

func TestLockDifferentIDs(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.3.0",
		Path:    "configs/.rr-lock-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		l,
		cfg,
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second)
	res, err := lock("foo", "bar", 20*secMult, 100*secMult)
	assert.True(t, res)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	res, err = release("foo", "bar1")
	assert.False(t, res)
	assert.NoError(t, err)

	time.Sleep(time.Second * 20)

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("lock request received").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("release called for the resource which is not owned by the caller").Len())
}

// race condition test, all methods are involved
func TestLockInit(t *testing.T) {
	cont := endure.New(slog.LevelInfo)

	cfg := &config.Plugin{
		Version: "2023.2.0",
		Path:    "configs/.rr-lock-init.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&logger.Plugin{},
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	resources := map[int]string{0: "foo", 1: "foo1", 2: "foo2", 3: "foo3", 4: "foo4", 5: "foo5"}

	for range 100 {
		rs := randomString(10)
		go func() {
			_, err1 := lock(resources[genRandNum(6)], rs, (genRandNum(5)+1)*secMult, (genRandNum(15)+1)*secMult)
			assert.NoError(t, err1)
		}()
		go func() {
			_, err2 := lock(resources[genRandNum(6)], randomString(3), (genRandNum(4)+1)*secMult, (genRandNum(11)+1)*secMult)
			assert.NoError(t, err2)
		}()
		go func() {
			_, err3 := lock(resources[genRandNum(6)], randomString(3), (genRandNum(2)+1)*secMult, (genRandNum(90)+1)*secMult)
			assert.NoError(t, err3)
		}()
		go func() {
			_, err4 := lock(resources[genRandNum(6)], randomString(3), (genRandNum(10)+1)*secMult, (genRandNum(10)+1)*secMult)
			assert.NoError(t, err4)
		}()
		go func() {
			_, err5 := lock(resources[genRandNum(6)], randomString(3), (genRandNum(20)+1)*secMult, (genRandNum(13)+1)*secMult)
			assert.NoError(t, err5)
		}()
		go func() {
			_, err6 := lock(resources[genRandNum(6)], randomString(3), (genRandNum(80)+1)*secMult, (genRandNum(10)+1)*secMult)
			assert.NoError(t, err6)
		}()
		go func() {
			_, err7 := lock(resources[genRandNum(6)], randomString(3), (genRandNum(20)+1)*secMult, (genRandNum(19)+1)*secMult)
			assert.NoError(t, err7)
		}()
		go func() {
			_, err8 := updateTTL(resources[genRandNum(6)], randomString(3), (genRandNum(5))*secMult)
			assert.NoError(t, err8)
		}()
		go func() {
			_, err9 := exists(resources[genRandNum(6)], rs)
			assert.NoError(t, err9)
		}()

		go func() {
			_, err10 := release(resources[genRandNum(6)], rs)
			assert.NoError(t, err10)
		}()

		go func() {
			_, err11 := lockRead(resources[genRandNum(6)], randomString(3), (genRandNum(20)+1)*secMult, (genRandNum(15)+1)*secMult)
			assert.NoError(t, err11)
		}()
		go func() {
			_, err12 := lockRead(resources[genRandNum(6)], randomString(3), (genRandNum(2)+1)*secMult, (genRandNum(34)+1)*secMult)
			assert.NoError(t, err12)
		}()
		go func() {
			_, err13 := lockRead(resources[genRandNum(6)], randomString(3), (genRandNum(20)+1)*secMult, (genRandNum(13)+1)*secMult)
			assert.NoError(t, err13)
		}()
		go func() {
			_, err14 := lockRead(resources[genRandNum(6)], randomString(3), (genRandNum(25)+1)*secMult, (genRandNum(15)+1)*secMult)
			assert.NoError(t, err14)
		}()
		go func() {
			_, err15 := lockRead(resources[genRandNum(6)], randomString(3), (genRandNum(20)+1)*secMult, (genRandNum(76)+1)*secMult)
			assert.NoError(t, err15)
		}()
		go func() {
			_, err16 := lockRead(resources[genRandNum(6)], randomString(3), (genRandNum(20)+1)*secMult, (genRandNum(15)+1)*secMult)
			assert.NoError(t, err16)
		}()
		go func() {
			_, err17 := forceRelease(resources[genRandNum(6)])
			assert.NoError(t, err17)
		}()
	}

	time.Sleep(time.Minute * 3)

	stopCh <- struct{}{}
	wg.Wait()
	time.Sleep(time.Second * 5)
}

func TestLockFromSeveralProcesses(t *testing.T) {
	cont := endure.New(slog.LevelInfo)

	cfg := &config.Plugin{
		Version: "2023.1.0",
		Path:    "configs/.rr-lock-init.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&logger.Plugin{},
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 2)
	answ := make([]int, 0, 4)
	mu := &sync.Mutex{}

	go func() {
		res, err := lock("foo", "bar", 5*secMult, 1*secMult)
		assert.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		switch res {
		case true:
			answ = append(answ, 1)
		case false:
			answ = append(answ, 0)
		}
	}()
	go func() {
		res, err := lock("foo", "bar", 5*secMult, 1*secMult)
		assert.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		switch res {
		case true:
			answ = append(answ, 1)
		case false:
			answ = append(answ, 0)
		}
	}()
	go func() {
		res, err := lock("foo", "bar", 5*secMult, 1*secMult)
		assert.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		switch res {
		case true:
			answ = append(answ, 1)
		case false:
			answ = append(answ, 0)
		}
	}()
	go func() {
		res, err := lock("foo", "bar", 5*secMult, 1*secMult)
		assert.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		switch res {
		case true:
			answ = append(answ, 1)
		case false:
			answ = append(answ, 0)
		}
	}()

	time.Sleep(time.Second * 10)

	stopCh <- struct{}{}
	wg.Wait()
	time.Sleep(time.Second * 2)

	mu.Lock()
	slices.Sort(answ)
	assert.Equal(t, []int{0, 0, 0, 1}, answ)
	mu.Unlock()
}

func TestLockReadInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.1.0",
		Path:    "configs/.rr-lock-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		l,
		cfg,
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)
	res, err := lock("foo", "bar", 5*secMult, 0)
	assert.True(t, res)
	assert.NoError(t, err)

	res, err = lockRead("foo", "bar", 0, 10*secMult)
	assert.True(t, res)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	wg2 := &sync.WaitGroup{}
	wg2.Go(func() {
		res2, err2 := lockRead("foo", "bar1", 0, 11*secMult)
		assert.True(t, res2)
		assert.NoError(t, err2)
	})

	wg2.Go(func() {
		res3, err3 := lockRead("foo", "bar2", 0, 11*secMult)
		assert.True(t, res3)
		assert.NoError(t, err3)
	})

	wg2.Wait()
	time.Sleep(time.Second)

	res, err = exists("foo", "bar1")
	assert.True(t, res)
	assert.NoError(t, err)
	res, err = exists("foo", "bar2")
	assert.True(t, res)
	assert.NoError(t, err)

	res, err = release("foo", "bar")
	assert.True(t, res)
	assert.NoError(t, err)
	res, err = release("foo", "bar1")
	assert.True(t, res)
	assert.NoError(t, err)
	res, err = release("foo", "bar2")
	assert.True(t, res)
	assert.NoError(t, err)

	stopCh <- struct{}{}
	wg.Wait()
	time.Sleep(time.Second * 2)

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("no such lock resource, creating new").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("waiting to acquire a lock, w==1, r==0").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("exists request received").Len())
	assert.Equal(t, 3, oLogger.FilterMessageSnippet("lock successfully released").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("returning releaseMuCh mutex to temporarily allow releasing locks").Len())
}

func TestLockUpdateTTL(t *testing.T) {
	cont := endure.New(slog.LevelInfo)

	cfg := &config.Plugin{
		Version: "2023.2.0",
		Path:    "configs/.rr-lock-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		l,
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	res, err := lock("foo", "bar", 1000*secMult, 0)
	assert.NoError(t, err)
	assert.True(t, res)

	res, err = updateTTL("foo", "bar", 2*secMult)
	assert.NoError(t, err)
	assert.True(t, res)

	res, err = lockRead("foo", "bar1", 0, 10*secMult)
	assert.NoError(t, err)
	assert.True(t, res)

	time.Sleep(time.Second * 3)

	res, err = release("foo", "bar1")
	assert.NoError(t, err)
	assert.True(t, res)

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("updateTTL request received").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("r/lock: ttl was updated").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("lock successfully released").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("r/lock: ttl removed, stop callback call").Len())
}

func TestForceRelease(t *testing.T) {
	cont := endure.New(slog.LevelInfo)

	cfg := &config.Plugin{
		Version: "2023.2.0",
		Path:    "configs/.rr-lock-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		l,
		cfg,
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	)
	require.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)
	res, err := lock("foo", "bar", 1000*secMult, 0)
	assert.NoError(t, err)
	assert.True(t, res)

	res, err = lockRead("foo", "bar1", 0, 1*secMult)
	assert.NoError(t, err)
	assert.False(t, res)

	res, err = forceRelease("foo")
	assert.NoError(t, err)
	assert.True(t, res)

	res, err = lockRead("foo", "bar1", 0, 10*secMult)
	assert.NoError(t, err)
	assert.True(t, res)

	time.Sleep(time.Second)

	res, err = exists("foo", "bar1")
	assert.NoError(t, err)
	assert.True(t, res)

	res, err = release("foo", "bar1")
	assert.NoError(t, err)
	assert.True(t, res)

	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("failed to acquire a readlock, timeout exceeded, w==1, r==0").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("all force-release messages were sent").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("lock successfully released").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("r/lock: ttl removed, stop callback call").Len())
}

// startLockContainer brings up rpc + lock + logger on 127.0.0.1:6001 and returns
// a stop function the test must defer. Used by the deterministic coverage tests
// below (the api-test helper lives in lock_api_test.go, which is not part of the
// coverage build).
func startLockContainer(t *testing.T) func() {
	t.Helper()

	cont := endure.New(slog.LevelError)
	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-lock-init.yaml",
	}

	require.NoError(t, cont.RegisterAll(
		cfg,
		&logger.Plugin{},
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	))
	require.NoError(t, cont.Init())

	ch, err := cont.Serve()
	require.NoError(t, err)

	wg := &sync.WaitGroup{}
	stop := make(chan struct{})
	wg.Go(func() {
		select {
		case e := <-ch:
			assert.NoError(t, e.Error, "container reported error")
		case <-stop:
		}
	})

	time.Sleep(time.Second) // let rpc bind 6001

	return func() {
		close(stop)
		require.NoError(t, cont.Stop())
		wg.Wait()
	}
}

// TestLockWaitThenAcquire covers the write-lock wait-then-acquire arm of lock():
// a second writer blocks on the notification channel and acquires the lock once
// the holder's TTL expires (rather than timing out, as the other tests do).
func TestLockWaitThenAcquire(t *testing.T) {
	defer startLockContainer(t)()

	// A holds the write lock; it expires on its own after ~2s.
	ok, err := lock("wta", "A", 2*secMult, 0)
	require.NoError(t, err)
	require.True(t, ok)

	// B blocks waiting for the lock (up to its 10s wait) and acquires it once
	// A's TTL expires, exercising the wait-then-acquire arm instead of timing out.
	ok, err = lock("wta", "B", 10*secMult, 10*secMult)
	require.NoError(t, err)
	require.True(t, ok, "B should acquire the lock after A expires")
}

// TestLockPromoteReadToWrite covers the read->write promotion arm of lock():
// a single read lock is promoted to a write lock by the same id.
func TestLockPromoteReadToWrite(t *testing.T) {
	defer startLockContainer(t)()

	ok, err := lockRead("promote", "X", 100*secMult, 0) // r=1, w=0
	require.NoError(t, err)
	require.True(t, ok)

	// Same id requests a write lock: the promotion arm signals the reader to
	// stop, waits for the notification, then takes the write lock.
	ok, err = lock("promote", "X", 10*secMult, 10*secMult)
	require.NoError(t, err)
	require.True(t, ok)
}

// TestExistsWildcard covers the exists() wildcard branch (id == "*"), which
// reports whether a resource holds any lock at all.
func TestExistsWildcard(t *testing.T) {
	defer startLockContainer(t)()

	ok, err := lock("wild", "Y", 100*secMult, 0)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = exists("wild", "*") // resource has a writer -> true
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = exists("absent", "*") // no such resource -> false
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = release("wild", "Y")
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(time.Second) // let the callback zero the counters

	ok, err = exists("wild", "*") // resource exists but holds no locks -> false
	require.NoError(t, err)
	require.False(t, ok)
}

const letterBytes = "abc"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[genRandNum(len(letterBytes))]
	}
	return string(b)
}

func genRandNum(upper int) int {
	bg := big.NewInt(int64(upper))

	n, err := rand.Int(rand.Reader, bg)
	if err != nil {
		panic(err)
	}

	return int(n.Int64())
}

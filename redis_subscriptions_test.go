package lock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisSubscriptionIOOutsideRegistry(t *testing.T) {
	for _, command := range []string{"subscribe", "unsubscribe"} {
		t.Run(command, func(t *testing.T) {
			r := subscriptionTestBackend(t)
			blocked, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			r.client.AddHook(subscriptionWriteHook{command: command, blocked: blocked, release: release})
			wake := make(chan struct{}, 1)
			registered := make(chan *channelWaiters, 1)
			go func() {
				group, _ := r.addWaiter("first", wake)
				registered <- group
			}()
			var group *channelWaiters
			select {
			case group = <-registered:
			case <-time.After(300 * time.Millisecond):
				t.Fatal("registration waited for a Pub/Sub socket write")
			}
			if command == "unsubscribe" {
				awaitSubscriptionEvent(t, group.ready, "initial subscription")
				removed := make(chan struct{})
				go func() {
					r.removeWaiter("first", group, wake)
					close(removed)
				}()
				awaitSubscriptionEvent(t, blocked, "blocked unsubscribe write")
				awaitSubscriptionEvent(t, removed, "waiter cleanup during a socket write")
			} else {
				awaitSubscriptionEvent(t, blocked, "blocked subscribe write")
			}

			other := make(chan *channelWaiters, 1)
			go func() {
				group, _ := r.addWaiter("second", make(chan struct{}, 1))
				other <- group
			}()
			select {
			case group := <-other:
				select {
				case <-group.ready:
					t.Fatal("a queued channel became ready before its subscription was written")
				default:
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("another resource waited for a Pub/Sub socket write")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if err := r.stop(ctx); err != nil {
				t.Fatalf("stop must close the blocked socket and join workers: %v", err)
			}
		})
	}
}

func awaitSubscriptionEvent(t *testing.T, event <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("timed out waiting for %s", name)
	}
}

type subscriptionWriteHook struct {
	command string
	blocked chan struct{}
	release <-chan struct{}
}

func (h subscriptionWriteHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		closed := make(chan struct{})
		return &subscriptionWriteConn{Conn: conn, hook: h, closed: closed, close: sync.OnceFunc(func() { close(closed) })}, nil
	}
}

func (subscriptionWriteHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (subscriptionWriteHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type subscriptionWriteConn struct {
	net.Conn
	hook   subscriptionWriteHook
	closed <-chan struct{}
	close  func()
}

func (c *subscriptionWriteConn) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte("\r\n"+c.hook.command+"\r\n")) {
		close(c.hook.blocked)
		select {
		case <-c.hook.release:
		case <-c.closed:
			return 0, net.ErrClosed
		}
	}
	return c.Conn.Write(b)
}

func (c *subscriptionWriteConn) Close() error {
	c.close()
	return c.Conn.Close()
}

// subscriptionTestBackend serves only connection setup and Pub/Sub commands.
func subscriptionTestBackend(t *testing.T) *redisBackend {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer func() { _ = conn.Close() }()
				stop := context.AfterFunc(t.Context(), func() { _ = conn.Close() })
				defer stop()
				reader := bufio.NewReader(conn)
				for {
					args, err := subscriptionTestCommand(reader)
					if err != nil {
						return
					}
					reply := "+OK\r\n"
					switch args[0] {
					case "hello":
						reply = "%0\r\n"
					case "ping":
						reply = "+PONG\r\n"
					case "subscribe", "unsubscribe":
						reply = fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:1\r\n", len(args[0]), args[0], len(args[1]), args[1])
					}
					if _, err = io.WriteString(conn, reply); err != nil {
						return
					}
				}
			})
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		workers.Wait()
	})
	r, err := newRedisBackend(slog.New(slog.NewTextHandler(io.Discard, nil)), RedisConfig{Addrs: []string{listener.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return r
}

func subscriptionTestCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range args {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		body := make([]byte, size+2)
		if _, err = io.ReadFull(reader, body); err != nil {
			return nil, err
		}
		args[i] = string(body[:size])
	}
	return args, nil
}

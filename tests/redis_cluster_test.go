package lock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisClusterLostScriptReply(t *testing.T) {
	for _, method := range []string{"Lock", "LockRead", "Release"} {
		for _, command := range []string{"evalsha", "eval"} {
			t.Run(method+"/"+command, func(t *testing.T) {
				admin := redisAdmin(t, 0)
				var executions atomic.Int32
				proxy := redisClusterProxy(t, nil, func(args []string, reply []byte) bool {
					if args[0] == command && args[4] != "exists" && bytes.HasPrefix(reply, []byte("*2\r\n:1\r\n")) {
						return executions.Add(1) != 1
					}
					return true
				})
				client, _ := lockRPCClient(t, clusterProxyConfig(proxy))
				observer, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
				request := &lockV1.Request{Resource: t.Name(), Id: "owner"}
				var warm lockV1.Response
				require.NoError(t, client.Call("lock.Exists", request, &warm))
				if method == "Release" {
					var held lockV1.Response
					require.NoError(t, observer.Call("lock.Lock", request, &held))
					require.True(t, held.GetOk())
				}
				if command == "eval" {
					require.NoError(t, admin.ScriptFlush(t.Context()).Err())
				}

				var response lockV1.Response
				err := client.Call("lock."+method, request, &response)
				assert.Error(t, err, "a lost cluster script reply must report an unknown outcome")
				assert.False(t, response.GetOk())
				require.EqualValues(t, 1, executions.Load(), "the proxy must discard a successful script reply")
				var observed lockV1.Response
				require.NoError(t, observer.Call("lock.Exists", request, &observed))
				assert.Equal(t, method != "Release", observed.GetOk(), "Redis applied the mutation before its reply was lost")
			})
		}
	}
}

func TestRedisClusterScriptRedirects(t *testing.T) {
	for _, redirect := range []string{"MOVED", "ASK"} {
		t.Run(redirect, func(t *testing.T) {
			admin := redisAdmin(t, 0)
			var fallbacks atomic.Int32
			target := redisClusterProxy(t, nil, func(args []string, _ []byte) bool {
				if args[0] == "eval" {
					fallbacks.Add(1)
				}
				return true
			})
			var redirects atomic.Int32
			proxy := redisClusterProxy(t, func(args []string) []byte {
				if args[0] == "evalsha" || args[0] == "eval" {
					redirects.Add(1)
					return fmt.Appendf(nil, "-%s 0 %s\r\n", redirect, target)
				}
				return nil
			}, nil)
			client, _ := lockRPCClient(t, clusterProxyConfig(proxy))
			require.NoError(t, admin.ScriptFlush(t.Context()).Err())
			var response lockV1.Response
			require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: t.Name(), Id: "owner"}, &response))
			require.True(t, response.GetOk(), "safe redirects and NOSCRIPT fallback must reach the slot owner")
			require.Positive(t, redirects.Load())
			require.EqualValues(t, 1, fallbacks.Load())
		})
	}
}

func clusterProxyConfig(addr string) *config.Plugin {
	return &config.Plugin{
		Type: "yaml",
		ReadInCfg: fmt.Appendf(nil,
			"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q, %q]}}", addr, addr),
	}
}

// redisClusterProxy supplies cluster routing replies and forwards commands to real Redis.
// keepReply can discard a reply by closing the caller connection after execution.
func redisClusterProxy(t *testing.T, intercept func([]string) []byte, keepReply func([]string, []byte) bool) string {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().(*net.TCPAddr)
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
					var reply []byte
					if intercept != nil {
						reply = intercept(args)
					}
					if reply == nil {
						switch {
						case args[0] == "cluster" && args[1] == "slots":
							reply = fmt.Appendf(nil, "*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:%d\r\n", addr.Port)
						case args[0] == "asking":
							reply = []byte("+OK\r\n")
						default:
							if _, err = server.Write(wire); err != nil {
								return
							}
							reply, err = readRedisFrame(replies)
							if err != nil {
								return
							}
						}
					}
					if keepReply != nil && !keepReply(args, reply) {
						return
					}
					if _, err = caller.Write(reply); err != nil {
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
	return listener.Addr().String()
}

func readRedisFrame(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil || len(line) < 3 {
		return nil, io.ErrUnexpectedEOF
	}
	switch line[0] {
	case '$', '=', '!', '*', '~', '>', '%':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return line, nil
		}
		if line[0] == '$' || line[0] == '=' || line[0] == '!' {
			body := make([]byte, n+2)
			_, err = io.ReadFull(reader, body)
			return append(line, body...), err
		}
		if line[0] == '%' {
			n *= 2
		}
		for range n {
			body, err := readRedisFrame(reader)
			if err != nil {
				return nil, err
			}
			line = append(line, body...)
		}
	}
	return line, nil
}

func redisCommandArgs(wire []byte) ([]string, error) {
	reader := bufio.NewReader(bytes.NewReader(wire))
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
		frame, err := readRedisFrame(reader)
		if err != nil {
			return nil, err
		}
		_, value, _ := bytes.Cut(frame, []byte("\r\n"))
		args[i] = string(value[:len(value)-2])
	}
	return args, nil
}

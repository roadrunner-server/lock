package lock

import (
	"context"
	"net"
	"sync"

	"github.com/redis/go-redis/v9"
)

type redisClientHook struct {
	ctx context.Context
}

func (h redisClientHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(h.ctx, cancel)
		defer stop()
		defer cancel()
		conn, err := next(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		// Closing the socket interrupts I/O even while PubSub holds its internal mutex.
		closeConn := sync.OnceValue(conn.Close)
		return &redisConn{Conn: conn, close: closeConn, stop: context.AfterFunc(h.ctx, func() { _ = closeConn() })}, nil
	}
}

func (redisClientHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "evalsha" || cmd.Name() == "eval" {
			cmd = redisNoRetryCmd{Cmder: cmd}
		}
		return next(ctx, cmd)
	}
}

func (redisClientHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type redisNoRetryCmd struct {
	redis.Cmder
}

// NoRetry blocks ambiguous cluster retries. MOVED and ASK still route the command.
func (redisNoRetryCmd) NoRetry() bool {
	return true
}

type redisConn struct {
	net.Conn
	stop  func() bool
	close func() error
}

func (c *redisConn) Close() error {
	c.stop()
	return c.close()
}

package lock

import (
	"context"
	stderr "errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
)

const defaultImmediateTimeout = time.Millisecond

// errEmptyID surfaces as connect.CodeInvalidArgument; all RPCs except ForceRelease
// require a caller-supplied ID to attribute ownership of the resource.
var errEmptyID = stderr.New("empty ID is not allowed")

type rpc struct {
	log *slog.Logger
	pl  *Plugin
}

// waitContext bounds the locker call by req.Wait microseconds, falling back to
// defaultImmediateTimeout when wait is zero. The parent ctx propagates the
// caller's deadline / cancellation into the lock acquire.
func waitContext(parent context.Context, waitUs int64) (context.Context, context.CancelFunc) {
	if waitUs == 0 {
		return context.WithTimeout(parent, defaultImmediateTimeout)
	}
	return context.WithTimeout(parent, time.Microsecond*time.Duration(waitUs))
}

func (r *rpc) Lock(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	msg := req.Msg
	r.log.Debug("lock request received", "ttl", int(msg.GetTtl()), "wait_ttl", int(msg.GetWait()), "resource", msg.GetResource(), "id", msg.GetId())

	if msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.lock(cctx, msg.GetResource(), msg.GetId(), int(msg.GetTtl())),
	}), nil
}

func (r *rpc) LockRead(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	msg := req.Msg
	r.log.Debug("read lock request received", "ttl", int(msg.GetTtl()), "wait_ttl", int(msg.GetWait()), "resource", msg.GetResource(), "id", msg.GetId())

	if msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.lockRead(cctx, msg.GetResource(), msg.GetId(), int(msg.GetTtl())),
	}), nil
}

func (r *rpc) Release(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	msg := req.Msg
	r.log.Debug("release request received", "ttl", int(msg.GetTtl()), "wait_ttl", int(msg.GetWait()), "resource", msg.GetResource(), "id", msg.GetId())

	if msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.release(cctx, msg.GetResource(), msg.GetId()),
	}), nil
}

func (r *rpc) ForceRelease(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	msg := req.Msg
	r.log.Debug("force release request received", "ttl", int(msg.GetTtl()), "wait_ttl", int(msg.GetWait()), "resource", msg.GetResource(), "id", msg.GetId())

	cctx, cancel := waitContext(ctx, msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.forceRelease(cctx, msg.GetResource()),
	}), nil
}

func (r *rpc) Exists(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	msg := req.Msg
	r.log.Debug("exists request received", "ttl", int(msg.GetTtl()), "wait_ttl", int(msg.GetWait()), "resource", msg.GetResource(), "id", msg.GetId())

	if msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.exists(cctx, msg.GetResource(), msg.GetId()),
	}), nil
}

func (r *rpc) UpdateTTL(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	msg := req.Msg
	r.log.Debug("updateTTL request received", "ttl", int(msg.GetTtl()), "wait_ttl", int(msg.GetWait()), "resource", msg.GetResource(), "id", msg.GetId())

	if msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.updateTTL(cctx, msg.GetResource(), msg.GetId(), int(msg.GetTtl())),
	}), nil
}

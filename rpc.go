package lock

import (
	"context"
	stderr "errors"
	"time"

	"connectrpc.com/connect"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
)

const defaultImmediateTimeout = time.Millisecond

var errEmptyID = stderr.New("empty ID is not allowed")

type rpc struct {
	pl *Plugin
}

func waitContext(parent context.Context, waitUs int64) (context.Context, context.CancelFunc) {
	if waitUs == 0 {
		return context.WithTimeout(parent, defaultImmediateTimeout)
	}
	return context.WithTimeout(parent, time.Microsecond*time.Duration(waitUs))
}

func (r *rpc) Lock(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	r.pl.log.Debug("lock request received", "ttl", int(req.Msg.GetTtl()), "wait_ttl", int(req.Msg.GetWait()), "resource", req.Msg.GetResource(), "id", req.Msg.GetId())

	if req.Msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, req.Msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.lock(cctx, req.Msg.GetResource(), req.Msg.GetId(), int(req.Msg.GetTtl())),
	}), nil
}

func (r *rpc) LockRead(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	r.pl.log.Debug("read lock request received", "ttl", int(req.Msg.GetTtl()), "wait_ttl", int(req.Msg.GetWait()), "resource", req.Msg.GetResource(), "id", req.Msg.GetId())

	if req.Msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, req.Msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.lockRead(cctx, req.Msg.GetResource(), req.Msg.GetId(), int(req.Msg.GetTtl())),
	}), nil
}

func (r *rpc) Release(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	r.pl.log.Debug("release request received", "ttl", int(req.Msg.GetTtl()), "wait_ttl", int(req.Msg.GetWait()), "resource", req.Msg.GetResource(), "id", req.Msg.GetId())

	if req.Msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, req.Msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.release(cctx, req.Msg.GetResource(), req.Msg.GetId()),
	}), nil
}

func (r *rpc) ForceRelease(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	r.pl.log.Debug("force release request received", "ttl", int(req.Msg.GetTtl()), "wait_ttl", int(req.Msg.GetWait()), "resource", req.Msg.GetResource(), "id", req.Msg.GetId())

	cctx, cancel := waitContext(ctx, req.Msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.forceRelease(cctx, req.Msg.GetResource()),
	}), nil
}

func (r *rpc) Exists(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	r.pl.log.Debug("exists request received", "ttl", int(req.Msg.GetTtl()), "wait_ttl", int(req.Msg.GetWait()), "resource", req.Msg.GetResource(), "id", req.Msg.GetId())

	if req.Msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, req.Msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.exists(cctx, req.Msg.GetResource(), req.Msg.GetId()),
	}), nil
}

func (r *rpc) UpdateTTL(ctx context.Context, req *connect.Request[lockV1.LockRequest]) (*connect.Response[lockV1.LockResponse], error) {
	r.pl.log.Debug("updateTTL request received", "ttl", int(req.Msg.GetTtl()), "wait_ttl", int(req.Msg.GetWait()), "resource", req.Msg.GetResource(), "id", req.Msg.GetId())

	if req.Msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyID)
	}

	cctx, cancel := waitContext(ctx, req.Msg.GetWait())
	defer cancel()

	return connect.NewResponse(&lockV1.LockResponse{
		Ok: r.pl.locks.updateTTL(cctx, req.Msg.GetResource(), req.Msg.GetId(), int(req.Msg.GetTtl())),
	}), nil
}

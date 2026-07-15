package lock

import (
	"context"
	"errors"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
)

const defaultImmediateTimeout = time.Millisecond

var errEmptyID = errors.New("empty ID is not allowed")

type rpc struct {
	pl *Plugin
}

func waitContext(parent context.Context, waitUs int64) (context.Context, context.CancelFunc) {
	if waitUs == 0 {
		return context.WithTimeout(parent, defaultImmediateTimeout)
	}
	return context.WithTimeout(parent, time.Microsecond*time.Duration(waitUs))
}

func (r *rpc) Lock(in *lockV1.LockRequest, out *lockV1.LockResponse) error {
	r.pl.log.Debug("lock request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	cctx, cancel := waitContext(context.Background(), in.GetWait())
	defer cancel()

	out.Ok = r.pl.locks.lock(cctx, in.GetResource(), in.GetId(), int(in.GetTtl()))
	return nil
}

func (r *rpc) LockRead(in *lockV1.LockRequest, out *lockV1.LockResponse) error {
	r.pl.log.Debug("read lock request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	cctx, cancel := waitContext(context.Background(), in.GetWait())
	defer cancel()

	out.Ok = r.pl.locks.lockRead(cctx, in.GetResource(), in.GetId(), int(in.GetTtl()))
	return nil
}

func (r *rpc) Release(in *lockV1.LockRequest, out *lockV1.LockResponse) error {
	r.pl.log.Debug("release request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	cctx, cancel := waitContext(context.Background(), in.GetWait())
	defer cancel()

	out.Ok = r.pl.locks.release(cctx, in.GetResource(), in.GetId())
	return nil
}

func (r *rpc) ForceRelease(in *lockV1.LockRequest, out *lockV1.LockResponse) error {
	r.pl.log.Debug("force release request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	cctx, cancel := waitContext(context.Background(), in.GetWait())
	defer cancel()

	out.Ok = r.pl.locks.forceRelease(cctx, in.GetResource())
	return nil
}

func (r *rpc) Exists(in *lockV1.LockRequest, out *lockV1.LockResponse) error {
	r.pl.log.Debug("exists request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	cctx, cancel := waitContext(context.Background(), in.GetWait())
	defer cancel()

	out.Ok = r.pl.locks.exists(cctx, in.GetResource(), in.GetId())
	return nil
}

func (r *rpc) UpdateTTL(in *lockV1.LockRequest, out *lockV1.LockResponse) error {
	r.pl.log.Debug("updateTTL request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	cctx, cancel := waitContext(context.Background(), in.GetWait())
	defer cancel()

	out.Ok = r.pl.locks.updateTTL(cctx, in.GetResource(), in.GetId(), int(in.GetTtl()))
	return nil
}

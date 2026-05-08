package lock

import (
	"context"
	"errors"
	"log/slog"
	"time"

	lockApi "github.com/roadrunner-server/api-go/v6/lock/v1"
)

const defaultImmediateTimeout = time.Millisecond

type rpc struct {
	log *slog.Logger
	pl  *Plugin
}

func (r *rpc) Lock(req *lockApi.LockRequest, resp *lockApi.LockResponse) error {
	r.log.Debug("lock request received", "ttl", int(req.GetTtl()), "wait_ttl", int(req.GetWait()), "resource", req.GetResource(), "id", req.GetId())

	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), defaultImmediateTimeout)
		defer cancel()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	acq := r.pl.locks.lock(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	if acq {
		resp.Ok = true
		return nil
	}

	resp.Ok = false
	return nil
}

func (r *rpc) LockRead(req *lockApi.LockRequest, resp *lockApi.LockResponse) error {
	r.log.Debug("read lock request received", "ttl", int(req.GetTtl()), "wait_ttl", int(req.GetWait()), "resource", req.GetResource(), "id", req.GetId())

	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), defaultImmediateTimeout)
		defer cancel()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	acq := r.pl.locks.lockRead(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	if acq {
		resp.Ok = true
		return nil
	}

	resp.Ok = false
	return nil
}

func (r *rpc) Release(req *lockApi.LockRequest, resp *lockApi.LockResponse) error {
	r.log.Debug("release request received",
		"ttl", int(req.GetTtl()),
		"wait_ttl", int(req.GetWait()),
		"resource", req.GetResource(),
		"id", req.GetId(),
	)

	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), defaultImmediateTimeout)
		defer cancel()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	resp.Ok = r.pl.locks.release(ctx, req.GetResource(), req.GetId())
	return nil
}

func (r *rpc) ForceRelease(req *lockApi.LockRequest, resp *lockApi.LockResponse) error {
	r.log.Debug("force release request received", "ttl", int(req.GetTtl()), "wait_ttl", int(req.GetWait()), "resource", req.GetResource(), "id", req.GetId())

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), defaultImmediateTimeout)
		defer cancel()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	resp.Ok = r.pl.locks.forceRelease(ctx, req.GetResource())
	return nil
}

func (r *rpc) Exists(req *lockApi.LockRequest, resp *lockApi.LockResponse) error {
	r.log.Debug("exists request received",
		"ttl", int(req.GetTtl()),
		"wait_ttl", int(req.GetWait()),
		"resource", req.GetResource(),
		"id", req.GetId(),
	)

	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), defaultImmediateTimeout)
		defer cancel()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	resp.Ok = r.pl.locks.exists(ctx, req.GetResource(), req.GetId())
	return nil
}

func (r *rpc) UpdateTTL(req *lockApi.LockRequest, resp *lockApi.LockResponse) error {
	r.log.Debug("updateTTL request received", "ttl", int(req.GetTtl()), "wait_ttl", int(req.GetWait()), "resource", req.GetResource(), "id", req.GetId())
	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), defaultImmediateTimeout)
		defer cancel()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	resp.Ok = r.pl.locks.updateTTL(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	return nil
}

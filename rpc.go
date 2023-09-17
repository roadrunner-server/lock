package lock

import (
	"context"
	"errors"
	"time"

	lockApi "github.com/roadrunner-server/api/v4/build/lock/v1beta1"
	"go.uber.org/zap"
)

type rpc struct {
	log *zap.Logger
	pl  *Plugin
}

func (r *rpc) Lock(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("lock request received", zap.Int("ttl", int(req.GetTtl())), zap.Int("wait_ttl", int(req.GetWait())), zap.String("resource", req.GetResource()), zap.String("id", req.GetId()))

	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond)
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

func (r *rpc) LockRead(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("read lock request received", zap.Int("ttl", int(req.GetTtl())), zap.Int("wait_ttl", int(req.GetWait())), zap.String("resource", req.GetResource()), zap.String("id", req.GetId()))

	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*100)
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

func (r *rpc) Release(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("release request received", zap.Int("ttl", int(req.GetTtl())), zap.Int("wait_ttl", int(req.GetWait())), zap.String("resource", req.GetResource()), zap.String("id", req.GetId()))
	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}
	resp.Ok = r.pl.locks.release(context.Background(), req.GetResource(), req.GetId())
	return nil
}

func (r *rpc) ForceRelease(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("force release request received", zap.Int("ttl", int(req.GetTtl())), zap.Int("wait_ttl", int(req.GetWait())), zap.String("resource", req.GetResource()), zap.String("id", req.GetId()))
	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}
	resp.Ok = r.pl.locks.forceRelease(context.Background(), req.GetResource())
	return nil
}

func (r *rpc) Exists(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("'exists' request received",
		zap.Int("ttl", int(req.GetTtl())),
		zap.Int("wait_ttl", int(req.GetWait())),
		zap.String("resource", req.GetResource()),
		zap.String("id", req.GetId()),
	)

	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	resp.Ok = r.pl.locks.exists(ctx, req.GetResource(), req.GetId())
	cancel()
	return nil
}

func (r *rpc) UpdateTTL(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("updateTTL request received", zap.Int("ttl", int(req.GetTtl())), zap.Int("wait_ttl", int(req.GetWait())), zap.String("resource", req.GetResource()), zap.String("id", req.GetId()))
	if req.GetId() == "" {
		return errors.New("empty ID is not allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	resp.Ok = r.pl.locks.updateTTL(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	cancel()
	return nil
}

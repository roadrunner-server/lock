package lock

import (
	"context"
	"time"

	lockApi "go.buf.build/protocolbuffers/go/roadrunner-server/api/lock/v1beta1"
	"go.uber.org/zap"
)

type rpc struct {
	log *zap.Logger
	pl  *Plugin
}

func (r *rpc) Lock(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("lock request received", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx = context.Background()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	acq := r.pl.locks.lock(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	if acq {
		r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = true
		return nil
	}

	r.log.Debug("failed to acquire lock, already acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
	resp.Ok = false
	return nil
}

func (r *rpc) LockRead(req *lockApi.Request, resp *lockApi.Response) error {
	r.log.Debug("read lock request received", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))

	var ctx context.Context
	var cancel context.CancelFunc

	switch req.GetWait() {
	case int64(0):
		ctx = context.Background()
	default:
		ctx, cancel = context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
		defer cancel()
	}

	acq := r.pl.locks.lockRead(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	if acq {
		r.log.Debug("rlock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = true
		return nil
	}

	r.log.Debug("failed to acquire rlock, already acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
	resp.Ok = false
	return nil
}

func (r *rpc) Release(req *lockApi.Request, resp *lockApi.Response) error {
	resp.Ok = r.pl.locks.release(context.Background(), req.GetResource(), req.GetId())
	return nil
}

func (r *rpc) ForceRelease(req *lockApi.Request, resp *lockApi.Response) error {
	resp.Ok = r.pl.locks.forceRelease(context.Background(), req.GetResource())
	return nil
}

func (r *rpc) Exists(req *lockApi.Request, resp *lockApi.Response) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	resp.Ok = r.pl.locks.exists(ctx, req.GetResource(), req.GetId())
	cancel()
	return nil
}
func (r *rpc) UpdateTTL(req *lockApi.Request, resp *lockApi.Response) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	resp.Ok = r.pl.locks.updateTTL(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	cancel()
	return nil
}

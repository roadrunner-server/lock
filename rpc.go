package lock

import (
	"context"
	"sync"
	"time"

	lockApi "go.buf.build/protocolbuffers/go/roadrunner-server/api/lock/v1beta1"
	"go.uber.org/zap"
)

type rpc struct {
	log *zap.Logger
	mu  sync.Mutex
	pl  *Plugin
}

func (r *rpc) Lock(req *lockApi.Request, resp *lockApi.Response) error {
	// fast-path, when wait is eq to 0
	r.log.Debug("lock request received", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
	if req.GetWait() == 0 {
		// locker
		acq := r.pl.locks.lock(context.Background(), req.GetResource(), req.GetId(), int(req.GetTtl()))
		if acq {
			r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
			resp.Ok = true
			return nil
		}

		r.log.Debug("failed to acquire lock, already acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = false
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond*time.Duration(req.GetWait()))
	defer cancel()
	acq := r.pl.locks.lock(ctx, req.GetResource(), req.GetId(), int(req.GetTtl()))
	if acq {
		r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = true
		return nil
	}

	return nil
}

func (r *rpc) LockRead(req *lockApi.Request, resp *lockApi.Response) error {
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
	// resp.Ok = r.pl.locks.exists(req.GetResource(), req.GetId())
	return nil
}
func (r *rpc) UpdateTTL(req *lockApi.Request, resp *lockApi.Response) error {
	// resp.Ok = r.pl.locks.updateTTL(req.GetResource(), req.GetId(), int(req.GetTtl()))
	return nil
}

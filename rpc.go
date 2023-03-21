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
		acq := r.pl.locks.lock(req.GetResource(), req.GetId(), int(req.GetTtl()))
		if acq {
			r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
			resp.Ok = true
			return nil
		}

		r.log.Debug("failed to acquire lock, already acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = false
		return nil
	}

	acq := r.pl.locks.lock(req.GetResource(), req.GetId(), int(req.GetTtl()))
	if acq {
		r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = true
		return nil
	}

	r.log.Debug("acquire attempt failed, retrying in 1s", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))

	timer := time.NewTicker(time.Second)
	defer timer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(req.GetWait()))
	defer cancel()

	r.log.Debug("waiting for the lock to acquire", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
	stopCh := make(chan bool, 1)

	go func() {
		for {
			select {
			// try
			case <-timer.C:
				if !r.pl.locks.lock(req.GetResource(), req.GetId(), int(req.GetTtl())) {
					r.log.Debug("acquire attempt failed, retrying in 1s", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
					continue
				}

				r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
				stopCh <- true
				return

			case <-ctx.Done():
				// wait exceeded, send false
				r.log.Debug("failed to acquire lock, wait timeout exceeded", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
				stopCh <- false
				return
			}
		}
	}()

	res := <-stopCh
	resp.Ok = res

	return nil
}

func (r *rpc) LockRead(req *lockApi.Request, resp *lockApi.Response) error {
	// fast-path, when wait is eq to 0
	r.log.Debug("lock request received", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
	if req.GetWait() == 0 {
		// locker
		acq := r.pl.locks.lockRead(req.GetResource(), req.GetId(), int(req.GetTtl()))
		if acq {
			r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
			resp.Ok = true
			return nil
		}

		r.log.Debug("failed to acquire lock, already acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = false
		return nil
	}

	if r.pl.locks.lockRead(req.GetResource(), req.GetId(), int(req.GetTtl())) {
		r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
		resp.Ok = true
		return nil
	}

	r.log.Debug("acquire attempt failed, retrying in 1s", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))

	timer := time.NewTicker(time.Second)
	defer timer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(req.GetWait()))
	defer cancel()

	r.log.Debug("waiting for the lock to acquire", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
	stopCh := make(chan bool, 1)

	go func() {
		for {
			select {
			// try
			case <-timer.C:
				if !r.pl.locks.lockRead(req.GetResource(), req.GetId(), int(req.GetTtl())) {
					r.log.Debug("acquire attempt failed, retrying in 1s", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
					continue
				}

				r.log.Debug("lock successfully acquired", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
				stopCh <- true
				return

			case <-ctx.Done():
				// wait exceeded, send false
				r.log.Debug("failed to acquire lock, wait timeout exceeded", zap.String("resource", req.GetResource()), zap.String("ID", req.GetId()))
				stopCh <- false
				return
			}
		}
	}()

	res := <-stopCh
	resp.Ok = res

	return nil
}

func (r *rpc) Release(req *lockApi.Request, resp *lockApi.Response) error {
	resp.Ok = r.pl.locks.release(req.GetResource(), req.GetId())
	return nil
}

func (r *rpc) ForceRelease(req *lockApi.Request, resp *lockApi.Response) error {
	resp.Ok = r.pl.locks.forceRelease(req.GetResource())
	return nil
}

func (r *rpc) Exists(req *lockApi.Request, resp *lockApi.Response) error {
	resp.Ok = r.pl.locks.exists(req.GetResource(), req.GetId())
	return nil
}
func (r *rpc) UpdateTTL(req *lockApi.Request, resp *lockApi.Response) error {
	resp.Ok = r.pl.locks.updateTTL(req.GetResource(), req.GetId(), int(req.GetTtl()))
	return nil
}

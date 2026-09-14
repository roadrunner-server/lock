package lock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
)

// The largest microsecond count which converts to a time.Duration.
const maxMicroseconds = int64(math.MaxInt64) / int64(time.Microsecond)

var (
	errEmptyID        = errors.New("empty ID is not allowed")
	errTTLOutOfRange  = fmt.Errorf("ttl must be between 0 and %d microseconds", maxMicroseconds)
	errWaitOutOfRange = fmt.Errorf("wait must be between 0 and %d microseconds", maxMicroseconds)
)

type rpc struct {
	pl *Plugin
}

func validateWait(wait int64) error {
	if wait < 0 || wait > maxMicroseconds {
		return errWaitOutOfRange
	}
	return nil
}

func validateTTLAndWait(ttl, wait int64) error {
	if ttl < 0 || ttl > maxMicroseconds {
		return errTTLOutOfRange
	}
	return validateWait(wait)
}

func (r *rpc) Lock(in *lockV1.Request, out *lockV1.Response) error {
	r.pl.log.Debug("lock request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	if err := validateTTLAndWait(in.GetTtl(), in.GetWait()); err != nil {
		return err
	}

	var err error
	out.Ok, err = r.pl.locks.lock(context.Background(), in.GetResource(), in.GetId(), in.GetTtl(), in.GetWait())
	return err
}

func (r *rpc) LockRead(in *lockV1.Request, out *lockV1.Response) error {
	r.pl.log.Debug("read lock request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	if err := validateTTLAndWait(in.GetTtl(), in.GetWait()); err != nil {
		return err
	}

	var err error
	out.Ok, err = r.pl.locks.lockRead(context.Background(), in.GetResource(), in.GetId(), in.GetTtl(), in.GetWait())
	return err
}

func (r *rpc) Release(in *lockV1.Request, out *lockV1.Response) error {
	r.pl.log.Debug("release request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	if err := validateWait(in.GetWait()); err != nil {
		return err
	}

	var err error
	out.Ok, err = r.pl.locks.release(context.Background(), in.GetResource(), in.GetId(), in.GetWait())
	return err
}

func (r *rpc) ForceRelease(in *lockV1.Request, out *lockV1.Response) error {
	r.pl.log.Debug("force release request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if err := validateWait(in.GetWait()); err != nil {
		return err
	}

	var err error
	out.Ok, err = r.pl.locks.forceRelease(context.Background(), in.GetResource(), in.GetWait())
	return err
}

func (r *rpc) Exists(in *lockV1.Request, out *lockV1.Response) error {
	r.pl.log.Debug("exists request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	if err := validateWait(in.GetWait()); err != nil {
		return err
	}

	var err error
	out.Ok, err = r.pl.locks.exists(context.Background(), in.GetResource(), in.GetId(), in.GetWait())
	return err
}

func (r *rpc) UpdateTTL(in *lockV1.Request, out *lockV1.Response) error {
	r.pl.log.Debug("updateTTL request received", "ttl", int(in.GetTtl()), "wait_ttl", int(in.GetWait()), "resource", in.GetResource(), "id", in.GetId())

	if in.GetId() == "" {
		return errEmptyID
	}

	if err := validateTTLAndWait(in.GetTtl(), in.GetWait()); err != nil {
		return err
	}

	var err error
	out.Ok, err = r.pl.locks.updateTTL(context.Background(), in.GetResource(), in.GetId(), in.GetTtl(), in.GetWait())
	return err
}

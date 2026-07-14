package lock

import (
	"context"
	"net"
	"net/rpc"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
)

const lockRPCAddr = "127.0.0.1:6001"

func newLockClient() (*rpc.Client, error) {
	conn, err := new(net.Dialer).DialContext(context.Background(), "tcp", lockRPCAddr)
	if err != nil {
		return nil, err
	}
	return rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn)), nil
}

func call(method string, in *lockV1.LockRequest) (bool, error) {
	cl, err := newLockClient()
	if err != nil {
		return false, err
	}
	defer func() { _ = cl.Close() }()

	out := &lockV1.LockResponse{}
	if err := cl.Call(method, in, out); err != nil {
		return false, err
	}
	return out.GetOk(), nil
}

func lock(resource, id string, ttl, wait int) (bool, error) {
	return call("lock.Lock", &lockV1.LockRequest{
		Resource: resource,
		Id:       id,
		Ttl:      new(int64(ttl)),
		Wait:     new(int64(wait)),
	})
}

func lockRead(resource, id string, ttl, wait int) (bool, error) {
	return call("lock.LockRead", &lockV1.LockRequest{
		Resource: resource,
		Id:       id,
		Ttl:      new(int64(ttl)),
		Wait:     new(int64(wait)),
	})
}

func release(resource, id string) (bool, error) {
	return call("lock.Release", &lockV1.LockRequest{
		Resource: resource,
		Id:       id,
	})
}

func updateTTL(resource, id string, ttl int) (bool, error) {
	return call("lock.UpdateTTL", &lockV1.LockRequest{
		Resource: resource,
		Id:       id,
		Ttl:      new(int64(ttl)),
	})
}

func forceRelease(resource string) (bool, error) {
	return call("lock.ForceRelease", &lockV1.LockRequest{
		Resource: resource,
	})
}

func exists(resource, id string) (bool, error) {
	return call("lock.Exists", &lockV1.LockRequest{
		Resource: resource,
		Id:       id,
	})
}

package lock

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"

	"connectrpc.com/connect"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/api-go/v6/lock/v1/lockV1connect"
	"golang.org/x/net/http2"
)

const lockRPCAddr = "127.0.0.1:6001"

func newLockClient() lockV1connect.LockServiceClient {
	httpc := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return new(net.Dialer).DialContext(ctx, network, addr)
			},
		},
	}
	return lockV1connect.NewLockServiceClient(httpc, "http://"+lockRPCAddr)
}

func lock(resource, id string, ttl, wait int) (bool, error) {
	resp, err := newLockClient().Lock(
		context.Background(),
		connect.NewRequest(&lockV1.LockRequest{
			Resource: resource,
			Id:       id,
			Ttl:      new(int64(ttl)),
			Wait:     new(int64(wait)),
		}),
	)
	if err != nil {
		return false, err
	}
	return resp.Msg.GetOk(), nil
}

func lockRead(resource, id string, ttl, wait int) (bool, error) {
	resp, err := newLockClient().LockRead(
		context.Background(),
		connect.NewRequest(&lockV1.LockRequest{
			Resource: resource,
			Id:       id,
			Ttl:      new(int64(ttl)),
			Wait:     new(int64(wait)),
		}),
	)
	if err != nil {
		return false, err
	}
	return resp.Msg.GetOk(), nil
}

func release(resource, id string) (bool, error) {
	resp, err := newLockClient().Release(
		context.Background(),
		connect.NewRequest(&lockV1.LockRequest{
			Resource: resource,
			Id:       id,
		}),
	)
	if err != nil {
		return false, err
	}
	return resp.Msg.GetOk(), nil
}

func updateTTL(resource, id string, ttl int) (bool, error) {
	resp, err := newLockClient().UpdateTTL(
		context.Background(),
		connect.NewRequest(&lockV1.LockRequest{
			Resource: resource,
			Id:       id,
			Ttl:      new(int64(ttl)),
		}),
	)
	if err != nil {
		return false, err
	}
	return resp.Msg.GetOk(), nil
}

func forceRelease(resource string) (bool, error) {
	resp, err := newLockClient().ForceRelease(
		context.Background(),
		connect.NewRequest(&lockV1.LockRequest{
			Resource: resource,
		}),
	)
	if err != nil {
		return false, err
	}
	return resp.Msg.GetOk(), nil
}

func exists(resource, id string) (bool, error) {
	resp, err := newLockClient().Exists(
		context.Background(),
		connect.NewRequest(&lockV1.LockRequest{
			Resource: resource,
			Id:       id,
		}),
	)
	if err != nil {
		return false, err
	}
	return resp.Msg.GetOk(), nil
}

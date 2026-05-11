package lock

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/api-go/v6/lock/v1/lockV1connect"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	lockPlugin "github.com/roadrunner-server/lock/v6"
	"github.com/roadrunner-server/logger/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const lockAPIAddr = "127.0.0.1:6001"

// startLockAPIContainer brings up rpc + lock + logger on lockAPIAddr.
// Returns a stop function the test must defer.
func startLockAPIContainer(t *testing.T) func() {
	t.Helper()

	cont := endure.New(slog.LevelError)
	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-lock-api.yaml",
	}

	require.NoError(t, cont.RegisterAll(
		cfg,
		&logger.Plugin{},
		&rpcPlugin.Plugin{},
		&lockPlugin.Plugin{},
	))
	require.NoError(t, cont.Init())

	ch, err := cont.Serve()
	require.NoError(t, err)

	wg := &sync.WaitGroup{}
	stop := make(chan struct{})
	wg.Go(func() {
		select {
		case e := <-ch:
			require.NoError(t, e.Error, "container reported error")
		case <-stop:
		}
	})

	time.Sleep(500 * time.Millisecond)

	return func() {
		close(stop)
		require.NoError(t, cont.Stop())
		wg.Wait()
	}
}

// TestLockConnectAPI exercises the lock RPCs through the Connect-RPC client
// (h2c). Go callers that import the generated lockV1connect package see
// exactly this wire shape.
func TestLockConnectAPI(t *testing.T) {
	stop := startLockAPIContainer(t)
	defer stop()

	httpc := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, network, addr)
			},
		},
	}
	client := lockV1connect.NewLockServiceClient(httpc, "http://"+lockAPIAddr)
	ctx := t.Context()

	const (
		resource = "connect-resource"
		id       = "connect-id"
	)
	ttl := int64(30 * time.Second / time.Microsecond)
	wait := int64(time.Second / time.Microsecond)

	resp, err := client.Lock(ctx, connect.NewRequest(&lockV1.LockRequest{
		Resource: resource,
		Id:       id,
		Ttl:      &ttl,
		Wait:     &wait,
	}))
	require.NoError(t, err)
	require.True(t, resp.Msg.GetOk())

	resp, err = client.Exists(ctx, connect.NewRequest(&lockV1.LockRequest{Resource: resource, Id: id}))
	require.NoError(t, err)
	require.True(t, resp.Msg.GetOk())

	resp, err = client.Release(ctx, connect.NewRequest(&lockV1.LockRequest{Resource: resource, Id: id}))
	require.NoError(t, err)
	require.True(t, resp.Msg.GetOk())

	resp, err = client.Exists(ctx, connect.NewRequest(&lockV1.LockRequest{Resource: resource, Id: id}))
	require.NoError(t, err)
	require.False(t, resp.Msg.GetOk())
}

// TestLockHTTPApi exercises the lock RPCs through plain HTTP/1.1 with a
// protojson body — the wire shape PHP clients use via Guzzle/curl
// (PHP has no Connect SDK).
func TestLockHTTPApi(t *testing.T) {
	stop := startLockAPIContainer(t)
	defer stop()

	httpc := &http.Client{Timeout: 30 * time.Second}
	ctx := t.Context()

	call := func(method string, in proto.Message, out proto.Message) {
		body, err := protojson.Marshal(in)
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://"+lockAPIAddr+"/lock.v1.LockService/"+method, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpc.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equalf(t, http.StatusOK, resp.StatusCode, "method=%s body=%s", method, respBody)
		require.NoError(t, protojson.Unmarshal(respBody, out))
	}

	const (
		resource = "http-resource"
		id       = "http-id"
	)
	ttl := int64(30 * time.Second / time.Microsecond)
	wait := int64(time.Second / time.Microsecond)

	var lockResp lockV1.LockResponse
	call("Lock", &lockV1.LockRequest{
		Resource: resource,
		Id:       id,
		Ttl:      &ttl,
		Wait:     &wait,
	}, &lockResp)
	require.True(t, lockResp.GetOk())

	var existsResp lockV1.LockResponse
	call("Exists", &lockV1.LockRequest{Resource: resource, Id: id}, &existsResp)
	require.True(t, existsResp.GetOk())

	var relResp lockV1.LockResponse
	call("Release", &lockV1.LockRequest{Resource: resource, Id: id}, &relResp)
	require.True(t, relResp.GetOk())

	var existsResp2 lockV1.LockResponse
	call("Exists", &lockV1.LockRequest{Resource: resource, Id: id}, &existsResp2)
	require.False(t, existsResp2.GetOk())
}

// TestLockHTTPGetIdempotency verifies which methods accept HTTP GET. Only
// Exists is marked `option idempotency_level = NO_SIDE_EFFECTS;` in the proto,
// so Connect generates a handler that accepts GET for it. Mutating methods
// stay POST-only, so GET against them returns 405 Method Not Allowed.
func TestLockHTTPGetIdempotency(t *testing.T) {
	stop := startLockAPIContainer(t)
	defer stop()

	body, err := protojson.Marshal(&lockV1.LockRequest{Resource: "probe", Id: "probe"})
	require.NoError(t, err)

	q := url.Values{}
	q.Set("encoding", "json")
	q.Set("base64", "1")
	q.Set("message", base64.URLEncoding.EncodeToString(body))

	cases := []struct {
		method     string
		wantStatus int
	}{
		{"Exists", http.StatusOK},
		{"Lock", http.StatusMethodNotAllowed},
		{"LockRead", http.StatusMethodNotAllowed},
		{"Release", http.StatusMethodNotAllowed},
		{"ForceRelease", http.StatusMethodNotAllowed},
		{"UpdateTTL", http.StatusMethodNotAllowed},
	}

	httpc := &http.Client{Timeout: 30 * time.Second}
	for _, c := range cases {
		t.Run(c.method, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				"http://"+lockAPIAddr+"/lock.v1.LockService/"+c.method+"?"+q.Encode(), nil)
			require.NoError(t, err)

			resp, err := httpc.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()

			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equalf(t, c.wantStatus, resp.StatusCode,
				"%s via GET -> %s\n%s", c.method, resp.Status, respBody)
		})
	}
}

// TestLockGRPCApi exercises the lock RPCs through a regular gRPC client
// (google.golang.org/grpc). The same Connect handler serves gRPC framing
// off the same port — used by PHP's gRPC extension.
func TestLockGRPCApi(t *testing.T) {
	stop := startLockAPIContainer(t)
	defer stop()

	conn, err := grpc.NewClient(lockAPIAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := lockV1.NewLockServiceClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const (
		resource = "grpc-resource"
		id       = "grpc-id"
	)
	ttl := int64(30 * time.Second / time.Microsecond)
	wait := int64(time.Second / time.Microsecond)

	lockResp, err := client.Lock(ctx, &lockV1.LockRequest{
		Resource: resource,
		Id:       id,
		Ttl:      &ttl,
		Wait:     &wait,
	})
	require.NoError(t, err)
	require.True(t, lockResp.GetOk())

	existsResp, err := client.Exists(ctx, &lockV1.LockRequest{Resource: resource, Id: id})
	require.NoError(t, err)
	require.True(t, existsResp.GetOk())

	relResp, err := client.Release(ctx, &lockV1.LockRequest{Resource: resource, Id: id})
	require.NoError(t, err)
	require.True(t, relResp.GetOk())

	existsResp, err = client.Exists(ctx, &lockV1.LockRequest{Resource: resource, Id: id})
	require.NoError(t, err)
	require.False(t, existsResp.GetOk())
}

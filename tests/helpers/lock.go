package helpers

import (
	"net"
	"net/rpc"
	"testing"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/require"
)

// RPCAddr is the rpc plugin listener declared by tests/configs/.rr-lock-init.yaml.
const RPCAddr = "127.0.0.1:6321"

// defaultWait bounds the calls that carry no wait of their own. Without it the
// plugin falls back to a 1ms budget for taking the global mutex, which is too
// tight to be reliable under -race.
const defaultWait = time.Second * 2

// LockClient calls the lock plugin over the rpc plugin. net/rpc clients are safe
// for concurrent use, so one client serves a whole test.
type LockClient struct {
	cl *rpc.Client
}

// NewLockClient dials the rpc listener. The client, and with it the connection,
// is closed by t.Cleanup.
func NewLockClient(t *testing.T) *LockClient {
	t.Helper()

	var d net.Dialer
	conn, err := d.DialContext(t.Context(), "tcp", RPCAddr)
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })

	return &LockClient{cl: client}
}

// Lock acquires an exclusive lock, waiting up to wait for a conflicting holder
// to go away.
func (c *LockClient) Lock(resource, id string, ttl, wait time.Duration) (bool, error) {
	return c.call("lock.Lock", &lockV1.Request{
		Resource: resource,
		Id:       id,
		Ttl:      micros(ttl),
		Wait:     micros(wait),
	})
}

// LockRead acquires a shared lock, waiting up to wait for a writer to go away.
func (c *LockClient) LockRead(resource, id string, ttl, wait time.Duration) (bool, error) {
	return c.call("lock.LockRead", &lockV1.Request{
		Resource: resource,
		Id:       id,
		Ttl:      micros(ttl),
		Wait:     micros(wait),
	})
}

// Release releases the lock held by id.
func (c *LockClient) Release(resource, id string) (bool, error) {
	return c.call("lock.Release", &lockV1.Request{
		Resource: resource,
		Id:       id,
		Wait:     micros(defaultWait),
	})
}

// ForceRelease releases every lock held on the resource, regardless of the owner.
func (c *LockClient) ForceRelease(resource string) (bool, error) {
	return c.call("lock.ForceRelease", &lockV1.Request{
		Resource: resource,
		Wait:     micros(defaultWait),
	})
}

// Exists reports whether id holds a lock on the resource. The id "*" asks
// whether the resource holds any lock at all.
func (c *LockClient) Exists(resource, id string) (bool, error) {
	return c.call("lock.Exists", &lockV1.Request{
		Resource: resource,
		Id:       id,
		Wait:     micros(defaultWait),
	})
}

// UpdateTTL replaces the TTL of the lock held by id. A zero ttl means unlimited.
func (c *LockClient) UpdateTTL(resource, id string, ttl time.Duration) (bool, error) {
	return c.call("lock.UpdateTTL", &lockV1.Request{
		Resource: resource,
		Id:       id,
		Ttl:      micros(ttl),
		Wait:     micros(defaultWait),
	})
}

// CallWithoutWait issues method with the wait field left unset, the request a
// client that omits it produces. The plugin then bounds the call by its own
// immediate timeout instead of a caller-supplied window.
func (c *LockClient) CallWithoutWait(method, resource, id string) (bool, error) {
	return c.call(method, &lockV1.Request{
		Resource: resource,
		Id:       id,
	})
}

func (c *LockClient) call(method string, in *lockV1.Request) (bool, error) {
	out := &lockV1.Response{}
	if err := c.cl.Call(method, in, out); err != nil {
		return false, err
	}

	return out.GetOk(), nil
}

// micros converts a duration to the microseconds the lock protocol carries.
func micros(d time.Duration) *int64 {
	return new(int64(d / time.Microsecond))
}

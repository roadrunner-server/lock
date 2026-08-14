package lock

import (
	"testing"

	"tests/helpers"

	lockPlugin "github.com/roadrunner-server/lock/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
)

// lockConfig is the only config of the suite: the rpc listener plus a logs section.
const lockConfig = "configs/.rr-lock-init.yaml"

// startLock boots the rpc and lock plugins and returns the container handle
// together with a client connected to the rpc listener.
func startLock(t *testing.T, opts ...helpers.Option) (*helpers.RR, *helpers.LockClient) {
	t.Helper()

	rr, _ := helpers.Start(t, lockConfig,
		[]any{&rpcPlugin.Plugin{}, &lockPlugin.Plugin{}},
		append(opts, helpers.WithTCPProbe(helpers.RPCAddr))...,
	)

	return rr, helpers.NewLockClient(t)
}

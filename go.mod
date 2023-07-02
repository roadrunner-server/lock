module github.com/roadrunner-server/lock/v4

go 1.20

require (
	go.buf.build/protocolbuffers/go/roadrunner-server/api v1.3.39
	go.uber.org/zap v1.24.0
)

replace github.com/roadrunner-server/sdk/v4 => ../../sdk

require (
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

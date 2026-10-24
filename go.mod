module caimandb

go 1.24

toolchain go1.24.4

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgraph-io/badger/v4 v4.9.4
	github.com/hashicorp/raft v1.7.3
	github.com/hashicorp/raft-boltdb/v2 v2.3.1
	github.com/klauspost/compress v1.19.1
	github.com/prometheus/client_golang v1.20.5
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.31.0
	golang.org/x/term v0.27.0
)

require (
	github.com/RoaringBitmap/roaring v1.9.4
	github.com/dgraph-io/ristretto/v2 v2.2.0
	github.com/google/uuid v1.6.0
	github.com/natefinch/atomic v1.0.1
	github.com/smallnest/ringbuffer v0.1.1
	github.com/vmihailenco/msgpack/v5 v5.4.1
	golang.org/x/sync v0.10.0
)

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/hashicorp/go-hclog v1.6.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	google.golang.org/protobuf v1.36.7 // indirect
)

exclude github.com/armon/go-metrics v0.3.8

exclude github.com/prometheus/common v0.9.1

exclude github.com/prometheus/procfs v0.0.0-20190507164030-5867b95ac084

exclude gopkg.in/alecthomas/kingpin.v2 v2.2.6

replace golang.org/x/sys => github.com/golang/sys v0.28.0

replace gopkg.in/check.v1 => github.com/go-check/check v0.0.0-20180628173108-788fd7840127

replace gopkg.in/yaml.v2 => github.com/go-yaml/yaml v0.0.0-20140924161607-9f9df34309c0

replace golang.org/x/sync => github.com/golang/sync v0.10.0

replace golang.org/x/net => github.com/golang/net v0.33.0

replace golang.org/x/text => github.com/golang/text v0.21.0

replace golang.org/x/mod => github.com/golang/mod v0.22.0

replace golang.org/x/tools => github.com/golang/tools v0.28.0

replace golang.org/x/time => github.com/golang/time v0.8.0

replace golang.org/x/oauth2 => github.com/golang/oauth2 v0.24.0

replace golang.org/x/xerrors => github.com/golang/xerrors v0.0.0-20231012003039-104605ab7028

replace go.uber.org/zap => github.com/uber-go/zap v1.27.0

replace go.uber.org/atomic => github.com/uber-go/atomic v1.11.0

replace go.uber.org/multierr => github.com/uber-go/multierr v1.11.0

replace golang.org/x/crypto => github.com/golang/crypto v0.31.0

replace golang.org/x/term => github.com/golang/term v0.27.0

replace golang.org/x/exp => github.com/golang/exp v0.0.0-20241217172543-b2144cdd0a67

replace golang.org/x/image => github.com/golang/image v0.23.0

replace gopkg.in/alecthomas/kingpin.v2 => github.com/alecthomas/kingpin v2.2.6+incompatible

replace golang.org/x/telemetry => github.com/golang/telemetry v0.0.0-20241220003058-cc96b6e0d3d9

replace google.golang.org/protobuf => github.com/protocolbuffers/protobuf-go v1.36.7

replace google.golang.org/grpc => github.com/grpc/grpc-go v1.68.1

replace go.opentelemetry.io/otel => github.com/open-telemetry/opentelemetry-go v1.37.0

replace go.opentelemetry.io/otel/metric => github.com/open-telemetry/opentelemetry-go/metric v1.37.0

replace go.opentelemetry.io/otel/trace => github.com/open-telemetry/opentelemetry-go/trace v1.37.0

replace go.opentelemetry.io/auto/sdk => github.com/open-telemetry/opentelemetry-go-instrumentation/sdk v1.1.0

replace go.uber.org/goleak => github.com/uber-go/goleak v1.3.0

replace go.etcd.io/bbolt => github.com/etcd-io/bbolt v1.3.5

replace gopkg.in/yaml.v3 => github.com/go-yaml/yaml v3.0.1+incompatible

replace cloud.google.com/go/compute/metadata => github.com/googleapis/google-cloud-go/compute/metadata v0.3.0

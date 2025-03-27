[![CI](https://github.com/clyso/chorus/actions/workflows/ci-go.yml/badge.svg)](https://github.com/clyso/chorus/actions/workflows/ci-go.yml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/clyso/chorus)
[![GoDoc](https://godoc.org/github.com/clyso/chorus?status.svg)](https://pkg.go.dev/github.com/clyso/chorus?tab=doc)
[![Go Report Card](https://goreportcard.com/badge/github.com/clyso/chorus)](https://goreportcard.com/report/github.com/clyso/chorus)

# Chorus
![chorus.png](./docs/media/banner.png)

Chorus is a distributed, vendor-agnostic, S3-compatible tool for backup, migration, and routing. It enables:
 - Faster data transfers between S3 storages using multiple machines.
 - Resumable transfers with checkpointing on failure.
 - Syncing of existing buckets, objects, and metadata (e.g., ACLs) from source to destination S3.
 - Real-time capture and propagation of bucket/object changes.
 - Routing of S3 requests to different storages based on user-defined rules.
 - Reduce downtime up to zero for switching to different S3 provider.

Listed features can be configured per S3 user and per bucket with [management CLI](./tools/chorctl), [REST](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/clyso/chorus/refs/heads/main/proto/gen/openapi/chorus/chorus.swagger.json)/[gRPC](./proto/chorus/chorus.proto) API, or [WebUI](./ui/).

## Components
[Chorus S3 Proxy](./service/proxy) service responsible for routing S3 requests and capturing data change events. 
[Chorus Agent](./service/agent) can be used as an alternative solution for capturing events instead of Proxy.
[Chorus Worker](./service/worker) service does actual data replication with the help of [RClone](https://github.com/rclone/rclone). Worker also hosts management API so it is a central and the only required component to start with Chorus.
Communication between Proxy/Agent and Worker is done over work queue. 
[Asynq](https://github.com/hibiken/asynq) with [Redis](https://github.com/redis/redis) is used as a work queue.

![diagram.png](./docs/media/diagram.png)

For more details, see:
- [Proxy](./service/proxy)
- [Worker](./service/worker)
- [Agent](./service/agent)
- [Management CLI](./tools/chorctl)
- [Web UI](./ui)
- [Standalone](./service/standalone) - all-in-one binary. Local playground with zero dependencies.

## Documentation

- Documentation available at [docs.clyso.com](https://docs.clyso.com/docs/products/chorus/overview).
- Project [Blog](https://docs.clyso.com/blog/tags/chorus/).

## Quick start

For a hands-on introduction, use the [docker-compose](./docker-compose) example. It includes a step-by-step README and sample configurations—ideal if you’re familiar with Docker and S3.

If using containers is not an option, try to run [standalone](./service/standalone) binary. It bundles all docker-compose services into one executable, letting you follow the same steps without containers.

## Installation

### From source

Requires [Go language](https://go.dev/doc/install). Clone and build with:
```shell
git clone https://github.com/clyso/chorus.git && cd chorus

# Run worker
go run ./cmd/worker

# Build worker binary
go build ./cmd/worker
./worker

# Run with custom config
go run ./cmd/worker -config <path-to-worker.yaml>
go run ./cmd/proxy -config <path-to-proxy.yaml>
go run ./cmd/agent -config <path-to-agent.yaml>
```

Or install globally:
```shell
# Ensure $GOPATH/bin is in $PATH
export PATH=$PATH:$GOPATH/bin

# Install binaries
go install github.com/clyso/chorus/cmd/worker@latest
go install github.com/clyso/chorus/cmd/proxy@latest
go install github.com/clyso/chorus/cmd/agent@latest
go install github.com/clyso/chorus/cmd/chorus@latest

# Run with config
worker -config <path-to-worker.yaml>
```

### Prebuilt Binaries
Download binaries for Linux, Windows, and macOS from [GitHub releases](https://github.com/clyso/chorus/releases) page.

### Docker

Multi-platform images are available at:
- `harbor.clyso.com/chorus/proxy`
- `harbor.clyso.com/chorus/worker`
- `harbor.clyso.com/chorus/agent`
- `harbor.clyso.com/chorus/web-ui`

Example:
```shell
docker run -v /path/to/worker.yaml:/bin/config/config.yaml harbor.clyso.com/chorus/worker
```

Built from [Dockerfile](./Dockerfile) and [ui/Dockerfile](./ui/Dockerfile).

See also [docker-compose](./docker-compose) example.

### Kubernetes
Deploy with Helm:
```shell
helm install <release name> oci://harbor.clyso.com/chorus/chorus
```
The chart source is located in [deploy/chorus](./deploy/chorus).

## Develop

Chorus is written in Go—no other dependencies are required to build, run, or test.

### Test
[Test](./test) package contains e2e tests for replications between S3 storages.
It starts:
- 3 embedded [gofakes3](https://github.com/johannesboyne/gofakes3) S3 storages
- embedded Redis [miniredis](https://github.com/alicebob/miniredis)
- Chorus Worker and Proxy services

All listed tools are written in go so test can be run without external dependencies just by:
```shell
go test ./test/...
```

Tests emulate real-world scenarios like:
- replication of existing buckets and objects with metadata
- capturing and syncing live bucket/object changes during migration
- switching between S3 endpoints with and without downtime
- access S3 with Chorus Proxy
- data migration integrity check

### Add APIs
Chorus worker [implements gRPC server](./pkg/api/) defined in [proto/chorus/chorus.proto](./proto/chorus/chorus.proto). It also provides REST API which is automatically generated from the proto file with [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway). GRPC<->REST mappings are defined in [proto/http.yaml](./proto/http.yaml).

Steps to add new API:
1. Add new service or endpoint definition to [proto/chorus/chorus.proto](./proto/chorus/chorus.proto)
2. Add corresponding mapping to [proto/http.yaml](./proto/http.yaml)
3. Run `./proto/gen_proto.sh` to generate go client, server, and openapi definitions to [proto/gen](./proto/gen) directory.
4. Implement new service or endpoint in [pkg/api](./pkg/api/)
5. Support new API in [chorctl](./tools/chorctl) and [WebUI](./ui) clients.

# Chorus
![chorus.png](./docs/media/banner.png)

Chorus is vendor-agnostic s3 backup, replication, and routing software. 
Once configured it can:
 - sync existing buckets and objects from source to destination s3 storage
 - capture live bucket/object changes from source s3
 - propagate the changes to destination s3

Listed features can be configured per s3 user and per bucket with [management CLI](./tools/chorctl) or webUI.

## Components
[Chorus S3 Proxy](./service/proxy) service responsible for routing s3 requests and capturing data change events. 
[Chorus Agent](./service/agent) can be used as an alternative solution for capturing events instead of proxy.
[Chorus Worker](./service/worker) service does actual data replication with the help of [RClone](https://github.com/rclone/rclone).
Communication between Proxy/Agent and worker is done over work queue. 
[Asynq](https://github.com/hibiken/asynq) with [Redis](https://github.com/redis/redis) is used as a work queue.

![diagram.png](./docs/media/diagram.png)

For more details, see:
- [proxy](./service/proxy)
- [worker](./service/worker)
- [agent](./service/agent)
- [standalone](./service/standalone)
- [management CLI](./tools/chorctl)

## Documentation

Documentation available at [docs.clyso.com](https://docs.clyso.com/docs/products/chorus/overview).

## Run
### From source
**REQUIREMENTS:**
- Go <https://go.dev/doc/install>

Run all-in-one [standalone binry](./service/standalone) from Go:
```shell
go run ./cmd/chorus
```

Or run each service separately:

**REQUIREMENTS:**
- Go <https://go.dev/doc/install>
- Redis <https://github.com/redis/redis>

```shell
# run chorus worker
go run ./cmd/worker

# run chorus worker with a custom yaml config file
go run ./cmd/worker -config <path to worker yaml config>

# run chorus proxy with a custom yaml config file
go run ./cmd/proxy -config <path to proxy yaml config>

# run chorus agent with a custom yaml config file
go run ./cmd/agent -config <path to agent yaml config>
```

### Standalone binary
See: [service/standalone](./service/standalone)

### Docker-compose
**REQUIREMENTS:**
- Docker

See: [docker-compose](./docker-compose)

### With Helm
**REQUIREMENTS:**
- K8s
- Helm

Install chorus helm chart from OCI registry:
```shell
helm install <release name> oci://harbor.clyso.com/chorus/chorus
```
See: [deploy/chours](./deploy/chorus)

## Develop

[test](./test) package contains e2e tests for replications between s3 storages.
It runs:
- 3 embedded [gofakes3](https://github.com/johannesboyne/gofakes3) s3 storages
- embedded redis [miniredis](https://github.com/alicebob/miniredis)
- worker and proxy service

all listed tools are written in go so test can be run without external dependencies just by:
```shell
go test ./test/...
```

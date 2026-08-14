# Chorus Helm Chart

Helm chart for deploying [Chorus](https://github.com/clyso/chorus) - a distributed, vendor-agnostic tool for backup, migration, and routing between S3 and OpenStack Swift storages.

## Installation

From OCI registry:
```shell
helm install <release-name> oci://harbor.clyso.com/chorus/chorus -f values.yaml
```

From source:
```shell
helm install <release-name> ./deploy/chorus -f values.yaml
```

## Examples

See the [examples/](./examples/) directory for ready-to-use configurations:

| Example | Description |
|---------|-------------|
| [values-s3.yaml](./examples/values-s3.yaml) | S3 to S3 replication |
| [values-swift.yaml](./examples/values-swift.yaml) | OpenStack Swift storage |
| [values-webhook.yaml](./examples/values-webhook.yaml) | Webhook-based replication (without proxy) |
| [values-dynamic-credentials.yaml](./examples/values-dynamic-credentials.yaml) | Manage credentials via API |
| [values-external-redis.yaml](./examples/values-external-redis.yaml) | External Redis configuration |
| [values-ingress-nginx.yaml](./examples/values-ingress-nginx.yaml) | Ingress overlay for ingress-nginx |
| [values-ingress-traefik.yaml](./examples/values-ingress-traefik.yaml) | Ingress overlay for Traefik |

## Configuration

Key sections in `values.yaml`:

### Storage

```yaml
storage:
  main: "main"  # Name of main storage (required)
  storages:
    main:
      type: S3  # S3 or SWIFT
      address: s3.example.com
      provider: Ceph  # Ceph, Minio, or Other
      isSecure: true
```

### Credentials

Stored in Kubernetes Secret, separate from storage config:

```yaml
credentials:
  storages:
    main:
      user1:
        accessKeyID: "..."
        secretAccessKey: "..."
```

### Dynamic Credentials

Manage credentials via API instead of config files:

```yaml
dynamicCredentials:
  enabled: true
  masterPassword: "..."  # or use existingSecret
```

### External Redis

```yaml
redis:
  enabled: false
externalRedis:
  addresses:
    - redis.example.com:6379
  existingSecret: "my-redis-secret"
```

### Metrics

`metrics.enabled` exposes a Prometheus `/metrics` endpoint on proxy and worker
(port 9090) and creates a `ClusterIP` metrics Service for each.

To scrape via the Prometheus Operator, also enable `serviceMonitor` and set the
discovery label your Prometheus selects on (check your operator's
`serviceMonitorSelector`):

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
    labels:
      release: kube-prometheus-stack  # match your Prometheus release
    interval: 30s        # optional
    scrapeTimeout: 10s   # optional
```

Enable `metrics` without `serviceMonitor` when scraping via pod annotations or a
ServiceMonitor managed elsewhere (e.g. on a cluster without the operator CRDs).

#### Exposed metrics

All metrics are served at `/metrics` on port `9090`. The `flow` label marks what
triggered the storage call: `event` (proxy/webhook change event), `migration`
(bucket or object migration task), or `api` (management API call).

Proxy:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `proxy_requests_total` | counter | `method` | Number of S3 requests to the proxy, by S3 method |
| `proxy_response_status` | counter | `status` | Proxy responses by HTTP status code |
| `proxy_response_time_seconds` | histogram | `method` | Proxy request duration (default buckets) |

Worker:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `worker_processed_tasks_total` | counter | `queue`, `task_type` | Tasks processed (both successful and failed) |
| `worker_failed_tasks_total` | counter | `queue`, `task_type` | Tasks whose processing returned an error |
| `worker_in_progress_tasks` | gauge | `queue`, `task_type` | Tasks currently being processed |
| `worker_task_duration_seconds` | histogram | `queue`, `task_type` | Task processing time (exponential buckets, 0.1s–600s) |
| `copy_in_progress_obj_bytes` | gauge | `flow`, `user`, `bucket` | Total size of objects currently being copied |
| `grpc_server_started_total`, `grpc_server_handled_total`, `grpc_server_msg_received_total`, `grpc_server_msg_sent_total` | counter | `grpc_service`, `grpc_method`, `grpc_type`, `grpc_code` | Management gRPC API, from [go-grpc-prometheus](https://github.com/grpc-ecosystem/go-grpc-prometheus) |

Proxy and worker (S3/Swift client calls):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `storage_requests_total` | counter | `flow`, `storage`, `method` | API calls made to a backing storage |
| `storage_bucket_bytes_upload` | counter | `flow`, `storage`, `bucket` | Bytes uploaded to a storage |
| `storage_bucket_bytes_download` | counter | `flow`, `storage`, `bucket` | Bytes downloaded from a storage |

Both components additionally expose the default collectors of the Prometheus Go
client: Go runtime metrics (`go_*`, see
[collectors.NewGoCollector](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/collectors#NewGoCollector))
and process metrics (`process_*`, see
[Prometheus process metrics](https://prometheus.io/docs/instrumenting/writing_clientlibs/#process-metrics)).

### Ingress

The chart can create four Ingresses, all disabled by default. Enable one with
`ingress.<name>.enabled: true` and at least one entry in `hosts`.

| Values key | Backend Service | Port (default) | Backend exists only when |
|------------|-----------------|----------------|--------------------------|
| `ingress.proxy` | `<release>-proxy` | 9669 | `proxy.enabled: true` |
| `ingress.api` | `<release>-rest` | 9671 | `worker.config.api.enabled: true` |
| `ingress.webhook` | `<release>-webhook` on separate webhook ports, else `<release>-rest` | `webhook.httpPort` / 9671 | `worker.config.api.webhook.enabled: true` |
| `ingress.ui` | `<release>-ui` | 9672 | `ui.enabled: true` |

Before enabling an ingress, make sure its component is enabled — see the last
column above.

Any Kubernetes ingress controller is supported: the chart emits no
controller-specific annotations. Set `className` for your controller and put
controller-specific tuning under `annotations`. See
[values-ingress-nginx.yaml](./examples/values-ingress-nginx.yaml) and
[values-ingress-traefik.yaml](./examples/values-ingress-traefik.yaml).

> **The management API has no authentication.** Anyone who can reach
> `ingress.api` can change replication policies, read the proxy's S3
> credentials, and write storage credentials when `dynamicCredentials.enabled`
> is set. Enforce authentication at the ingress, or keep it on a private
> network.

Set `worker.config.api.webhook.grpcPort` / `httpPort` to run the webhook on its
own ports and Service. Do that when exposing the webhook publicly: on the shared
ports its ingress backend is `<release>-rest`, which also serves the management
API described above. Set `worker.config.api.webhook.baseUrl` to the externally
reachable URL when the storage pushes events from outside the cluster.

### Images

Each component's image is set via `<component>.image` (`repository`, `tag`,
`pullPolicy`). `tag` defaults to the chart's `appVersion`, and `pullPolicy`
defaults to `IfNotPresent`. If you override `tag` with a mutable tag (e.g.
`latest` or a dev tag), set `pullPolicy: Always` so nodes don't keep a stale
cached image.

## Components

| Component | Description | Default |
|-----------|-------------|---------|
| **proxy** | S3 proxy for request routing and change capture | enabled |
| **worker** | Processes replication tasks | enabled |
| **ui** | Web dashboard | disabled |

## Documentation

- [Chorus Documentation](https://chorus.clyso.com/)
- [CLI Reference (chorctl)](https://github.com/clyso/chorus/tree/main/tools/chorctl)

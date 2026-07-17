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

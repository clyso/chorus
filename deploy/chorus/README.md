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

## Components

| Component | Description | Default |
|-----------|-------------|---------|
| **proxy** | S3 proxy for request routing and change capture | enabled |
| **worker** | Processes replication tasks | enabled |
| **ui** | Web dashboard | disabled |

## Documentation

- [Chorus Documentation](https://chorus.clyso.com/)
- [CLI Reference (chorctl)](https://github.com/clyso/chorus/tree/main/tools/chorctl)

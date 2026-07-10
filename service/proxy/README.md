# Chorus Proxy

Serves S3 proxy on port `:9669`. Routes S3 requests to storages from [config](./config.yaml) based on routing policy and creates replication tasks for [worker](../worker) based on replication policy.

## Usage

Set storage and Redis credentials in [config.yaml](./config.yaml) and run.

> [!IMPORTANT]
> Both Proxy and [Worker](../worker) should have the same Redis and storage configuration.

Manage replication policies with [CLI](../../tools/chorctl).

> [!NOTE]
> [Worker](../worker) is required to use CLI. Deploy worker and provide worker gRPC API address to CLI.

With [CLI](../../tools/chorctl):
- Check current replication state: `chorctl dash`
- List buckets for replication: `chorctl repl buckets -f <from> -t <to> -u <user>`
- Create replication: `chorctl repl add -f <from> -t <to> -u <user> -b <bucket>`

## Configuration

See [config.yaml](./config.yaml) for all options.

### Swift Note

For Swift storage, Proxy configuration differs from Worker:
- **Worker** needs `authURL` and full Keystone credentials
- **Proxy** only needs `storageURL` (direct Swift endpoint)

When using Helm, specify Swift storage separately in `proxy.config.storage` and `worker.config.storage`.

### Auth

For S3, Proxy supports S3 signature v4 (v2 can be enabled). See `auth` section in [config.yaml](./config.yaml):
- `auth.useStorage` - use credentials from a configured storage
- `auth.custom` - use custom credentials for proxy endpoint

For SWIFT, proxy does not perform authentication itself. It checks response status from forwarded requests to Swift storage.

### Credential aliases

Unlike the Worker, the Proxy supports multiple S3 credentials per user. Proxy S3 storage
credentials are nested `user -> alias -> credential`:

```yaml
storage:
  storages:
    one:
      address: s3.example.com
      credentials:
        user1:            # user (routing/replication policy key)
          laptop:         # alias
            accessKeyID: AKIA1
            secretAccessKey: ...
          ci:
            accessKeyID: AKIA2
            secretAccessKey: ...
```

The proxy authenticates callers by alias access key, keeps routing and replication policy
keyed by user only, and re-signs forwarded requests with the target storage's credential of
the same alias name. `auth.custom` uses the same nested form; each custom `(user, alias)`
pair must exist in the main storage credentials.

Aliases can also be added at runtime via the management API
(`chorctl set-user --alias <name> ...`) - see [docs/dynamic-creds.md](../../docs/dynamic-creds.md).

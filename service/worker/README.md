# Chorus Worker

Processes replication tasks from [Asynq](https://github.com/hibiken/asynq) work queue.
Hosts management [gRPC](../../proto/chorus/chorus.proto) API on port `:9670` and [REST](../../proto/gen/openapi/chorus/chorus.swagger.json) API on port `:9671`.

## Usage

Set storage and Redis credentials in [config.yaml](./config.yaml) and run.

The config file contains commented examples for:
- S3 storage configuration
- Swift storage configuration (requires `authURL` and Keystone credentials)
- Dynamic credentials (manage credentials via API instead of config)

## Configuration

See [config.yaml](./config.yaml) for all options with examples.

Key settings:
- `storage.storages` - S3 and Swift storage definitions with credentials
- `storage.dynamicCredentials` - enable API-based credential management
- `api.enabled` - enable management API (required for chorctl)
- `concurrency` - max parallel replication tasks

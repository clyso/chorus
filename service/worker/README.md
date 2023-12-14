# Chorus worker

Processes replication tasks from [Asynq](https://github.com/hibiken/asynq) work queue.
Hosts management [GRPC](../../proto/chorus/chorus.proto) api on port `:9670` and the same management api but
in [REST](../../proto/gen/openapi/chorus/chorus.swagger.json) format on port `:9671`.

Worker using [rclone](https://github.com/rclone/rclone) to copy object content and additionally sync tags and ACLs.

## Usage

Set s3 storage and redis credentials to [config](./config.yaml) and run.


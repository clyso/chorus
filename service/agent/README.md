# Chorus agent

Serves webhook on port `:9673` to receive push notifications from s3.
Then converts received s3 bucket notification to chorus replication task and puts it into chorus work queue to be later
processed by [Chorus worker](../worker).
Can be used as alternative to [Chorus proxy](../proxy).

## Usage

Set proper `url`, `fromStorage`, and `redis` credentials to [config](./config.yaml) and deploy agent to be reachable by source S3
storage. 

> [!IMPORTANT]  
> Make sure that `chorus-worker` and `chorus-agent` are using the same Redis instance.

> [!NOTE]  
> [Worker](../worker) is required to use CLI. Deploy worker and provide worker GRPC api address to CLI.

With [CLI](../../tools/chorctl):
- check that agent successfully registered itself and can be listed with CLI:
  ```shell
  chorctl agent
  ```
- use `FROM_STORAGE` and `PUSH_URL` from previous command output to create replication policy:
```shell
chorctl repl add --from="<src_storage_name>" --to="<dest_storage_name>" --user="<s3 user>" --bucket="<bucket name>" --agent-url="PUSH_URL"
```
- Chorus will automatically configure bucket notifications in soruce storage for given bucket and url (tested with aws and ceph rgw).
- now all changes from src storage will be propagated to dest

## See also

- Amazon S3 bucket
  notifications [documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)
- Ceph RGW S3 bucket notifications [documentation](https://docs.ceph.com/en/latest/radosgw/notifications/)

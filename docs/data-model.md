# Chorus data model

All chorus components are stateless and heavily relying on Redis for coordination and persistence. At the moment, there are no tenants in chorus, meaning that one installation of chorus should have its own redis DBs. Chorus installation needs 4 databases with the following defaults from [config](../pkg/config/config.yaml):

```yaml
redis:
  appDB: 0
  queueDB: 1
  lockDB: 2
  configDB: 3
```

Next sections will provide detailed overview of data layout in Redis per use-case.
- `<>` indicates variable params
- `[]` indicates optional params
- `KV` Redis [Key value](https://redis.io/docs/latest/develop/data-types/strings/)
- `SET` Redis [set](https://redis.io/docs/latest/develop/data-types/sets/)
- `SSET` Redis [sorted set](https://redis.io/docs/latest/develop/data-types/sorted-sets/)
- `HASH` Redis [hash tables](https://redis.io/docs/latest/develop/data-types/hashes/)
- `->` key - value separator: `a->b` - key `a` maps to value `b`

## 1. Work queue

DB: `queueDB`.

Chorus is using Redis-based work queue implementation named [Asynq](https://github.com/hibiken/asynq). It stores tasks in a dedicated Redis DB `queueDB`.

## 2. Distributed locks

DB: `lockDB`.

Locks are used to not sync the same object by multiple workers in parallel. 
Locks are stored in dedicated `lockDB`, where lock key is unique ID based on object type:

- `KV` `lk:<storage>:<bucket>:<object>` - represents bucket and used by object sync workers
- `KV` `lkb:<storage>:<bucket>` - represents bucket and used by bucket sync workers
- `KV` `lku:<storage>:<user>` - represents user and used by management API operations

Relevant source code is located in [/pkg/lock/service.go](../pkg/lock/service.go).

## 3. Temporary storage

DB: `appDB`.

Redis is used to store temporary information which should be shared between components or survive component restart:

- `KV` `s:<from storage>:<to storage>:<from bucket>[:<to bucket>][:<s3 ListObjects prefix>]` - persists progress in ListObjects operation during initial sync. Bucket may have millions of objects so this parameter is used for pagination to share between workers and not lose progress on worker restart.
- `SET` `s:up:<user>:<bucket>` - Redis Set storing multipart upload IDs during replication switch.

Relevant source code is located in [/pkg/storage/service.go](../pkg/storage/service.go).

## 4. Sync version metadata

DB: `appDB`.

When chorus captures bucket/object change event or syncs bucket in initial replication, it creates a task. This task contains only bucket/object id and no information about the change. Task processor then compares objects directly between source and destination and copy/remove data if needed. This makes tasks idempotent and works well with "at least once" delivery model used by many distributed queues.

Additionally, Chorus using [version vectors](https://en.wikipedia.org/wiki/Version_vector) to track synced changes. When proxy or agent captures change, it increases object version in source. Later, after worker syncs change, it updates version in destination.

Version metadata was introduced for two reasons:

1. reduce number of S3 API calls during sync
2. correctly resolve read requests on proxy during replication switch

Version vectors stored in Redis as  `HASH`, where key is unique bucket/object id and values are versions in destinations. Destination encodes storage name and optional bucket name (for custom destination bucket): `<storage>[:<bucket>]`:

```
my-bucket:my-object -> {        # obj key
   "storage1" -> 5              # obj version in source storage
   "storage2" -> 3              # obj version in dest storage
   "storage4:other-bucket" -> 4 # obj version in destination storage with custom bucket name
}

```

Here is a list of used version vectors:

- `<bucket name>:<obj name>[:<obj s3 version name>]`   - s3 object version metadata
- `<bucket name>:<obj name>[:<obj s3 version name>]:t` - s3 object version tags metadata
- `<bucket name>:<obj name>[:<obj s3 version name>]:a` - s3 object version ACL metadata
- `b:<bucket name>`   - s3 bucket metadata
- `b:<bucket name>:t` - s3 bucket tags metadata
- `b:<bucket name>:a` - s3 bucket ACL metadata

Relevant source code is located in [/pkg/meta/version_service.go](../pkg/meta/version_service.go).

## 5. Routing and Replication policies

DB: `configDB`.

Routing and replication can be configured per-bucket. Replications can be added/removed/paused/resumed. The configuration is stored in Redis and managed by user via gPRC API.

Relevant source code is located in [/pkg/policy/service.go](../pkg/policy/service.go).

### 5.1 Routing

- `KV` `p:route:<user> -> <storage>` - user-level routing policy 
- `KV` `p:route:<user>:<bucket> -> <storage>` - bucket-level routing policy. Overrides user policy if presented
- `SET` `p:route_block:<storage> -> <bucket>` - set of blocked buckets. Bucket is blocked if it is used as replication destination with custom bucket name.

Routing logic:
1. get target storage name from bucket-level routing
2. if not found then get from user-level routing
3. check if storage bucket is blocked
4. if zero donwtime replication switch is in progress adjust target storage based on version vector info for read requests. And based on uploadID for multipart request.

### 5.2 Replication

- `SSET` `p:repl:<user> -> <from storage>:<to storage> -> <task priority 0-5>` - user-level replication policy. This policy allows to start sync automatically for newly created buckets. When such new bucket is created, chorus automatically creates bucket-level replication and starts sync. Sorted set allows to store integer along with string value. String stores source and destination storage names and integer stores task priority. Priority allows customizing sync scheduling per-bucket. Here `<from storage>` is always the same for all set members, meaning that replication is one-to-many relationship.
- `SSET` `p:repl:<user>:<bucket> -> <from storage>:<to storage>[:<to bucket>] -> <task priority 0-5>` - bucket-level replication policy. Defines replication source and destination. Each replicated bucket has one. Chorus using these polices to start initial replication and to create proper replication tasks on receiving new data change event. Data layout is similar to user-level policy. The only difference is optional destiantion bucket name to support replication to custom bucket.
- `HASH` `p:repl_st:<user>:<from bucket>:<from storage>:<to storage>[:<to bucket>]` - bucket replication status metadata. Each bucket-level policy has corresponding status `HASH`. Status stores migration progress. See `ReplicationPolicyStatus` struct in [pkg/policy/service.go](../pkg/policy/service.go). Workers use status to support policy remove/pause/resume feature.
- `HASH` `p:switch:<user>:<bucket>` - bucket switch. Created when and if source and destination were switched in proxy. Holds information about previous source and switch status info. Relevant only for switch-replication feature. Cannot be used if replication was done to custom destination bucket. See struct `ReplicationSwitch` in [pkg/policy/service.go](../pkg/policy/service.go).

Replication policy creation logic:

1. Lookup routing policy and check that replication source equals to routing target.
2. Add bucket replication policy to `SSET` if not exists. If exists exit.
3. Create `HASH` with replication status.
4. Enqueue task to start replication.


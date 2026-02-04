# Swift Agent Design Document

## 1. Problem Statement

Chorus replicates data for object storages like S3 and Swift. It can copy existing data and capture ongoing data changes to replicate changes that occur during initial data replication.

Capturing ongoing changes serves two use cases:
- **Migration**: Reduce downtime when migrating to a different storage
- **Backup**: Continuous data backup to a different storage

Currently, Chorus captures data changes through two mechanisms:
1. [Chorus Proxy](../../service/proxy/): Intercepts S3/Swift requests directly
2. [Chorus Agent](../../service/agent/): Receives [S3 bucket notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html) via webhook

The S3 agent approach does not work for OpenStack Swift because Swift does not support the S3 bucket notification API. This document evaluates alternatives for implementing a Swift-compatible agent.

### 1.1 Requirements

**Functional Requirements**

| ID | Requirement |
|----|-------------|
| F1 | Capture object change events (create,update,delete, obj metadata update, multipart SLO/DLO) |
| F2 | Capture container change events (create, delete, metadata update) |
| F3 | Support filtering events by account and/or container (per replication policy) |

**Non-Functional Requirements**

| ID | Requirement | Target | Rationale |
|----|-------------|--------|-----------|
| N1 | Event capture latency | < 10 seconds | Chorus replicates asynchronously. Data copy takes time. Capture latency is tolerable when it is small compared to object copy time |
| N2 | Minimal impact on Swift hot path | No added latency to user requests, agent failure does not lead to Swift failure | Production safety |
| N3 | Deployment without Swift source modification | Preferred | Operational simplicity |
| N4 | Support Kubernetes deployment | Required | Primary deployment model |
| N5 | Support non-containerized deployment | Should | Customer flexibility |
| N6 | Extensibility to other storages (Ceph RGW) | Should | Broader applicability |

### 1.2 Context: How Chorus Agent Integrates

Chorus uses a policy-based replication model:

1. User creates replication via Chorus API specifying: `user`, `from_storage`, `to_storage`, optionally `from_bucket`/`to_bucket`
2. Replication policy stored in Redis
3. Agent receives events and queries policy service to determine if event matches active replication
4. For matching events, agent creates tasks in work queue
5. Worker processes tasks idempotently: compares source/destination, copies if needed. It is tolerable to duplicated/reordered tasks.

The [existing S3 agent](../../service/agent/http_handler.go) expects events in S3 bucket notification format:
- Account/user identifier
- Bucket name
- Object key
- Event type (Put, Delete, Copy, CompleteMultipartUpload)


<details>

<summary>S3 notification structure</summary>

```json
{  
   "Records":[  
      {  
         "eventName":"ObjectCreated:Put",
         "s3":{  
            "bucket":{  
               "name":"amzn-s3-demo-bucket",
               "ownerIdentity":{  
                  "principalId":"A3NL1KOZZKExample"
               },
               "arn":"arn:aws:s3:::amzn-s3-demo-bucket"
            },
            "object":{  
               "key":"HappyFace.jpg",
               "size":1024,
               "eTag":"d41d8cd98f00b204e9800998ecf8427e",
               "versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko",
               "sequencer":"0055AED6DCD90281E5"
            }
          ...
         }
        ...
      }
      ...
   ]
}
```

Full example: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html>

</details>


The Swift agent must extract equivalent information from available integration points.

## 2. Swift Integration Points

Swift provides two integration points for capturing data changes: middleware and access logs.

### 2.1 Middleware

Swift is a [WSGI application](https://docs.openstack.org/swift/latest/development_middleware.html) using Python's Paste framework. Middleware wraps request/response processing and is configured in `proxy-server.conf`:

```
[pipeline:main]
pipeline = catch_errors gatekeeper healthcheck proxy-logging cache ... proxy-server
```

**Implementation complexity**: Middleware requires Python code deployed into Swift's proxy-server process. A minimal notification middleware (~100 lines) would:
1. Intercept PUT/POST/DELETE requests
2. After successful response, send HTTP notification to Chorus agent
3. Handle timeouts/failures gracefully to avoid blocking Swift

**Deployment**: Requires modifying `proxy-server.conf` and restarting Swift proxy. Middleware packaged as Python wheel, installed via pip or system package.

**Existing effort**: [ENOSS](https://github.com/xvasil03/enoss) implements S3-compatible notifications for Swift. Status: 1 star, 68 commits, last activity ~4 years ago.

### 2.2 Access Logs

Swift logs all requests via the [`proxy_logging` middleware](https://docs.openstack.org/swift/latest/logs.html). Log format is configurable via `log_msg_template` parameter.

**Default format** includes: client_ip, timestamp, method, path, status, bytes, transaction_id, request_time.

**Available fields**:
```
{client_ip} {remote_addr} {end_time.datetime} {method} {path} {protocol}
{status_int} {referer} {user_agent} {auth_token} {bytes_recvd}
{bytes_sent} {client_etag} {transaction_id} {headers} {request_time}
{source} {log_info} {start_time} {end_time} {policy_index}
{account} {container} {object}
```

**Sample Swift log** (collected from test environment):
```
Feb  3 10:55:29 bebdc3f90a3d swift: ::ffff:172.19.0.1 ::ffff:172.19.0.1 03/Feb/2026/10/55/29 PUT /v1/AUTH_d3e36f6e2a6c44b1b4417b1ebc83492d/test-obj-meta-bucket/test-obj-meta-object HTTP/1.0 201 - gophercloud/v2.7.0 gAAAAABpgdQdLWt5... 12 - 9749fad13d6e7092a6337c4af9d83764 txef200785ab7b42459b59f-006981d421 - 0.0113 - - 1770116129.309001207 1770116129.320282936 0
```

Path structure: `/v1/AUTH_<project_id>/<container>/<object>`

**Key characteristic**: 
- Log format is not fixed. Each Swift installation may configure different templates, requiring configurable parsing.
- Log does not contain Swift method, only HTTP method and path. Swift method (Object/Container create/update/metadata-update/delete) must be derived from HTTP method + path.

### 2.3 Extensibility: Ceph RGW

A log-based approach extends to Ceph RGW, which provides [ops logging](https://docs.ceph.com/en/reef/radosgw/config-ref/) in JSON format:

**RGW S3 ops log** (sample):
```json
{"bucket":"test","object":"test.txt","time":"2026-02-03T14:34:07.882773Z",
 "operation":"put_obj","uri":"PUT /test/test.txt HTTP/1.1","http_status":"200",
 "user":"1b931c35175e404e9cb799962370c55a","bytes_received":1860,"object_size":1860,
 "authentication_type":"Keystone","access_key_id":"a7f1e798b7c2417cba4a02de97dc3cdc"}
```

**RGW Swift API ops log** (sample):
```json
{"bucket":"test-obj-meta-bucket","object":"test-obj-meta-object",
 "operation":"put_obj","uri":"PUT /swift/v1/AUTH_610acf37e0144593b76a8ce6a16f2c4f/test-obj-meta-bucket/test-obj-meta-object HTTP/1.1",
 "http_status":"201","user":"610acf37e0144593b76a8ce6a16f2c4f","object_size":12}
```

**Key characteristic**:
- Unlike OpenStack, RGW logs the Swift/S3 method name in the `operation` field.

## 3. Comparison

| Criterion | Middleware | Log Parsing |
|-----------|------------|-------------|
| **Production risk** | Higher (on hot path, bug affects Swift) | Lower (passive observer) |
| **Event reliability** | High (in request path) | Medium (depends on rotation handling) |
| **Deployment** | Swift config change + restart | Sidecar/file access only |
| **Latency** | Lower | Higher |
| **Format configuration** | Not needed | Required |
| **Extensibility** | Works only with OpenStack | Can be extended to support logs from other vendors |
| **Maintenance** | Two codebases (+ Python) | Single codebase |

## 4. Recommendation: Access Log Parsing

### 4.1 Rationale

Log parsing is recommended based on trade-off analysis:

1. **Production safety** (high weight): Log parsing cannot cause Swift outages. A middleware bug on the hot path risks production availability.
2. **Deployment simplicity** (high weight): No Swift configuration changes. Sidecar deployment is standard Kubernetes practice.
3. **Extensibility** (medium weight): Same architecture supports multiple storage vendors with different parser configuration.
4. **Maintenance** (medium weight): Go-only implementation aligns with Chorus codebase. No Python component to maintain.
5. **Latency trade-off** (acceptable): 1-10 second latency meets async replication requirements.
6. **Durability trade-off** (acceptable): Modern log tailing/exporting libraries handle log rotation/truncation correctly.

**When to reconsider middleware**:
- Latency requirements become < 1 second
- Event loss from log parsing proves unacceptable in practice
- A well-maintained notification middleware becomes available

## 5. Architecture

Agent architecture is inspired by popular log collector [FluentBit](https://fluentbit.io/how-it-works/)

```
[ Input ] → [ Parser ] → [ Filter ] → [ Buffer ] → [ Output ]
```

1. Agent parses log entry to map it to S3 notification structure with:
   - [Swift method](../../pkg/swift/methods.go)
   - Resource name: Account/Container/[Object]/[ObjectVersion]
2. Agent filters out read requests and errors.
3. Agent batches multiple entries into buffer.
4. Agent sends batch of events to Chorus webhook.


**Alternatives:**

Agent can be redesigned to directly create replication tasks for chorus worker instead of sending S3-like notification webhook.
There are 2 drawbacks:
- Task queue client requires Redis connection, meaning that Redis credentials should be distributed to log collectors and Redis port should be available outside of Chorus worker deployment.
- Agent will have to query replication policies to filter events, increasing complexity of agent logic.


### 5.1 File Tailing

Handling log file tailing is critical for event durability. Reusing existing libraries is recommended.

**Tail Library Options**:
- [OTel stanza/fileconsumer](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/stanza/fileconsumer) - Most sophisticated: fingerprint-based file identity (survives renames), handles both copy/truncate and move/create rotation, concurrent file support. Battle-tested in OpenTelemetry Collector.
- [nxadm/tail](https://github.com/nxadm/tail) or [un000/tailor](https://github.com/un000/tailor) - Simpler alternatives if OTel dependency is too heavy.

All options handle position tracking, inode detection, and rotation - avoiding reimplementation of these complex behaviors.

### 5.2 Log Parsing

Log format is different for each storage vendor:
- RGW produces JSON logs containing all needed fields, including Swift method. No parsing needed.
- OpenStack Swift produces text logs which have to be parsed. Moreover, log format is configurable per installation.

However, the main difficulty is not variable format, but mapping HTTP method + path to Swift operation (object/container create/update/delete). 
This mapping logic could be described in a DSL, or using regexes, but would be complex and error-prone. Most importantly, it would burden users with understanding Swift internals.

**Recommendation**: The agent uses predefined **source types** that encapsulate the mapping logic for each storage vendor. Users configure only the log parsing; the event classification (method + path → event type) is hardcoded per source.

```yaml
swift_agent:
  source: openstack_swift
  log_path: /var/log/swift/proxy.log
  config:
    log_msg_template: # paste Swift log format template from swift config or keep empty for default
```

```yaml
swift_agent:
  source: rgw
  log_path: /var/log/ceph/rgw-ops.log
  config: {} # not needed for RGW JSON logs
```

**Rationale**:
- Mapping logic is well-defined per vendor (e.g., Swift PUT + 4 path segments = ObjectCreated)
- Users shouldn't need to understand or configure this mapping
- Keeps configuration simple; complex logic stays in testable Go code
- Trade-off: adding new vendor requires code change, but this is infrequent and ensures correctness

### 5.3 Alternative: parse logs with Fluent Bit

Use Fluent Bit for log tailing and parsing, then send parsed events to Chorus agent via HTTP.
Fluent Bit allows to use Lua scripts or regex parsers to extract needed fields and map to Swift method.

---
Below is example Fluent Bit config for Swift logs:

<details>

<summary>Fluent Bit config + Lua script</summary>


> [!WARNING]
> Config and script are illustrative only. It was generated using AI. Full implementation and testing is needed.


```ini
[PARSER]
  Name swift_proxy
  Format regex
  Regex ^(?<sys_time>\w+\s+\d+\s+\d+:\d+:\d+)\s+(?<host>\S+)\s+swift:\s+(?<remote_addr>\S+)\s+(?<xff>\S+)\s+(?<req_time>\d+\/\w+\/\d+\/\d+\/\d+\/\d+)\s+(?<method>GET|PUT|POST|HEAD|DELETE)\s+(?<path>\/v1\/[^ ]+)\s+HTTP\/(?<http_ver>[0-9.]+)\s+(?<status>\d{3})\s+
[INPUT]
  Name   tail
  Path   /var/log/swift/proxy.log
  Parser swift_proxy
  Tag    swift.access
[FILTER]
  Name   lua
  Match  swift.access
  Script swift_to_s3.lua
  Call   map_record
[OUTPUT]
  Name   http
  Match  swift.access
  Host   webhook.example.com
  Port   443
  URI    /swift-events
  Format json
  Header Content-Type application/json
```

The Lua script `swift_to_s3.lua` would implement the mapping logic from HTTP method + path to Swift method and construct S3-like notification JSON.

```lua
local batch = {}
local BATCH_SIZE = 10

function map_record(tag, ts, record)
  local path = record["path"]
  local method = record["method"]
  local status = tonumber(record["status"])

  -- Filter: only process successful requests (2xx)
  if not status or status < 200 or status >= 300 then
    return -1, ts, record
  end

  if not path or not method then
    return -1, ts, record
  end

  -- Filter: only mutations (ignore GET, HEAD)
  if method == "GET" or method == "HEAD" then
    return -1, ts, record
  end

  -- Parse path: /v1/AUTH_x/container/object
  local parts = {}
  for p in string.gmatch(path, "[^/]+") do
    table.insert(parts, p)
  end

  local account = parts[2]    -- AUTH_xxx
  local container = parts[3]
  local object = parts[4]     -- may be nil for container ops

  -- Extract account ID from AUTH_xxx prefix
  if account and string.sub(account, 1, 5) == "AUTH_" then
    account = string.sub(account, 6)
  end

  local op = nil

  if object then
    -- Object operations (4+ path segments)
    if method == "PUT" then op = "ObjectCreated"
    elseif method == "POST" then op = "ObjectMetadataUpdated"
    elseif method == "DELETE" then op = "ObjectDeleted"
    end
  elseif container then
    -- Container operations (3 path segments)
    if method == "PUT" then op = "ContainerCreated"
    elseif method == "POST" then op = "ContainerMetadataUpdated"
    elseif method == "DELETE" then op = "ContainerDeleted"
    end
  end

  if not op then
    return -1, ts, record
  end

  local event = {
    eventTime = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    swiftOperation = op,
    swift = {
      account = account,
      container = container,
      object = object
    }
  }

  table.insert(batch, event)

  if #batch >= BATCH_SIZE then
    local payload = {
      Records = batch
    }
    batch = {}
    return 1, ts, payload
  end

  return -1, ts, record
end
```
</details>

---

### 5.4 Decision: Built-in vs Fluent Bit

| Criterion | Built-in Agent | Fluent Bit + Webhook |
|-----------|----------------|----------------------|
| **Deployment** | Go sidecar | Fluent Bit sidecar |
| **Configuration** | Single YAML file | Fluent Bit config + Lua script + Chorus webhook |
| **Log format sync** | Can parse Swift `log_msg_template` directly | Must manually write regex matching the template |
| **Testing** | Go unit tests for mapping logic | Lua script harder to test in CI |
| **Maintenance** | More Go code to write | Less code, but Lua is separate language |
| **User familiarity** | Chorus-specific config | Fluent Bit widely known in ops community |

**Recommendation**: ???

# Swift migration

Similar to S3 migration, SWIFT migration consists of two stages:
1. **Initial migration**: List all existing containers and objects in the source, then copy them to the destination in the background.
2. **Live migration**: Capture all data changes during the initial migration and apply them to the destination.

For a detailed overview of the S3 migration process, see the [article](https://docs.clyso.com/blog/s3-migration-with-chorus/) and the [step-by-step guide](https://docs.clyso.com/docs/products/chorus/usage/#s3-migration).

## 1. Initial migration

Workflow per account:
1. copy src account metadata to dest
2. list all containers in src account
3. create containers in dest and copy container metadata
4. list all objects in src containers
5. copy all objects with meta to dest
6. **TODO: how to resolve cross-container/cross-account dependencies - SLO,DLO,symlinks???**

## 2. Live migration

Chorus Proxy will capture all data changes made to the source SWIFT storage during the initial migration and produce the following tasks for Chorus workers:

```mermaid
---
title: Capturing Live data changes
--- 
%%{init: { 'sequence': {'noteAlign':'left'} } }%%
sequenceDiagram
    autonumber
    Actor u as User
    Participant p as Chorus<br>Proxy
    Participant s as SWIFT<br>Source
    Participant d as SWIFT<br>Destination
    Participant w as Chorus<br>Worker

    note over u,w: capture event
    u->>+p: write request<br>POST,PUT,DELETE
    p->>+s: forward
    s->>-p: 200 ok
    p-)w: create task
    Note left of w: - account<br>- [opt]container<br>- [opt]object<br>- [opt]Last-Modified<br>- [opt]ETag
    p->>-u: 200 ok

    note over u,w: process event
    w-)w: process task<br>asynchronously<br>...
```

### Account
Handles **ONLY** account metadata updates.
Account creation is handled by the initial migration. Account deletion is ignored because it can be done only by admin.

| Method | Description |
|--------|-------------|
| [POST](https://docs.openstack.org/api-ref/object-store/?expanded=#create-update-or-delete-account-metadata) | create, update, or delete account metadata |
| [DELETE](https://docs.openstack.org/api-ref/object-store/?expanded=#delete-the-specified-account) | IGNORED - admin only |

- Q: Is it OK to use Swift `X-Newest: true` header?
- Q: Is it possible that dest acc not exists?

```mermaid
---
title: Sync account changes
--- 
 %%{init: { 'sequence': {'noteAlign':'left'} } }%%
sequenceDiagram
    autonumber
    Participant s as SWIFT<br>Source
    Participant d as SWIFT<br>Destination
    Participant w as Chorus<br>Worker

    note over w: produced by POST<br>account event
    w-)+w: enqueue task
    activate w
    Note right of w: - acc ID
    w->>+s: HEAD src account
    note right of s: X-Newest: true
    s->>-w: acc meta
    w->>+d: POST dst account
    d->>-w: 200 ok
    deactivate w
```

### Container
Handles container, Creation, Metadata update, and deletion:

| Method | Description |
|--------|-------------|
| [PUT](https://docs.openstack.org/api-ref/object-store/?expanded=#create-container) | creates container |
| [POST](https://docs.openstack.org/api-ref/object-store/?expanded=#create-update-or-delete-container-metadata) | CRUD container metadata |
| [DELETE](https://docs.openstack.org/api-ref/object-store/?expanded=#delete-container) | deletes container |

- Q: It is possible that the source container is deleted, but the destination container is not empty and cannot be deleted. This can happen if the delete object task is processed after the delete container task (out of order). What should we do?
  - a) Retry the delete container task later (simple, but may cause a deadlock if the delete object task is lost due to a bug or outage).
  - b) Delete all objects in the container and try to delete the container again (may take a long time if there are many objects).
  - c) Create an async task to delete all objects, then retry the delete container task later.
  - d) Use [swift bulk delete](https://docs.openstack.org/swift/latest/api/bulk-delete.html). Is it supported by all swift implementations?
- Q: Should we support [container synchronization](https://docs.openstack.org/swift/latest/overview_container_sync.html)? What if container `X-Container-Sync-To` was not migrated to the destination? Should we exclude sync destination container from migration because it will by synced by swift anyway?

```mermaid
---
title: Sync container changes
--- 
 %%{init: { 'sequence': {'noteAlign':'left'} } }%%
sequenceDiagram
    autonumber
    Participant s as SWIFT<br>Source
    Participant d as SWIFT<br>Destination
    Participant w as Chorus<br>Worker

    note over w: produced by<br>PUT,POST,DELETE<br>container event
    w-)w: enqueue task
    activate w
    Note right of w: - acc ID<br>- container
    w->>+s: HEAD src container
    note right of s: X-Newest: true

    opt container was updated
      s->>-w: 200: acc meta
      w->>+d: POST dest container
      d-x-w: 200 ok - exit
    end
    deactivate w

    opt container was deleted
      activate w
      s->>w: 404: not found
      w->>+d: DELETE dest container
      alt empty dest container
        d-x-w: 200 ok - exit
        deactivate w
      else non-empty dest container
        activate w
        d->>w: 409 Conflict: dest container not empty
        note right of w: delete obj<br>tasks were<br>not processed
        w->>w: retry later
        deactivate w
      end
    end
```

### Object

Unlike accounts and containers, Chorus creates a separate task for each Swift object operation and handles them individually. Each Swift object sync task is shown in a separate diagram below.

| Method | Description | Task Type |
|--------|-------------|------|
| [POST](https://docs.openstack.org/api-ref/object-store/?expanded=#create-or-update-object-metadata) | create or update object metadata | [`ObjectMetaUpdate`](#object-meta-update) |
| [PUT](https://docs.openstack.org/api-ref/object-store/?expanded=#create-or-update-object) | create or update object payload | [`ObjectUpdate`](#object-payload-update) |
| [COPY](https://docs.openstack.org/api-ref/object-store/?expanded=#copy-object) | copy object | [`ObjectUpdate`](#object-payload-update) same as for `PUT` |
| [DELETE](https://docs.openstack.org/api-ref/object-store/?expanded=#delete-object) | delete object | [`ObjectDelete`](#object-delete) |

- Q: In S3, objects are independent. In SWIFT, objects can be linked via SLO, DLO, or symlinks. How should this be handled?
   - a) Use a K8s controller-style approach: retry all tasks until all objects are synced. Consider mechanisms to detect and resolve deadlocks.
   - b) Indicate object dependencies in tasks and control task execution order in the queue—this is likely impossible.

#### Object meta update

- Q: if object was not found in src, but exists in dest, should we delete it?

```mermaid
---
title: Sync object meta-only changes
--- 
 %%{init: { 'sequence': {'noteAlign':'left'} } }%%
sequenceDiagram
    autonumber
    Participant s as SWIFT<br>Source
    Participant d as SWIFT<br>Destination
    Participant w as Chorus<br>Worker

    note over w: produced by<br>POST object event
    w-)w: enqueue task
    activate w
    Note right of w: - acc ID<br>- container<br>- object
    w->>+s: HEAD src object
    note right of s: X-Newest: true

    alt object exists
      s->>-w: 200: object meta
      w->>+d: POST dest object
      d-x-w: 200 ok - exit
      deactivate w
    else object not found
      activate w
      s-xw: 404: not found
      deactivate w
      note right of w: do nothig<br>delete obj task<br>was not processed
    end
```

#### Object payload update

Object payload update task is produced by `PUT` or `COPY` object event. 
The Chorus worker does not distinguish between `PUT` and `COPY`, because for `COPY`, the destination storage might not have the latest object update.
Therefore, both operations are handled by the same task type.

open questions: 
- slo dlo `multipart-manifest`. See: https://docs.openstack.org/swift/latest/overview_large_objects.html how to sync manifest if parts not yet synced? Retry?
- X-Delete-At
- symlinks
- Q: how to handle versioning?

```mermaid
---
title: Sync object payload changes
--- 
 %%{init: { 'sequence': {'noteAlign':'left'} } }%%
sequenceDiagram
    autonumber
    Participant s as SWIFT<br>Source
    Participant d as SWIFT<br>Destination
    Participant w as Chorus<br>Worker

    note over w: produced by<br>PUT,COPY<br>object event
    w-)w: enqueue task
    activate w
    Note right of w: - acc ID<br>- container<br>- object<br>- Last-Modified<br>- ETag<br>- [opt]VersionID
    w->>+s: GET src object
    note right of s: If-Modified-Since: Last-Modified

    opt obj was updated
      s->>-w: 200: obj meta & payload
      w->>+d: PUT dest obje
      d-x-w: 200 ok - exit
    end
    deactivate w

    opt object was not updated
      activate w
      s->>w: 304: Not Modified
      note right of w: swift<br>inconsistent
      w->>w: retry later
      deactivate w
    end
    opt object was deleted
      activate w
      s-xw: 404: not found
      deactivate w
      note right of w: do nothig<br>delete obj task<br>was not processed
    end
```

#### Object delete

From [swift slo & dlo docs](https://docs.openstack.org/swift/latest/overview_large_objects.html):

> A DELETE request will just delete the manifest object itself. The segment data referenced by the manifest will remain unchanged.
>
> A DELETE with a query parameter:
>
> ?multipart-manifest=delete
> will delete all the segments referenced in the manifest and then the manifest itself. The failure response will be similar to the bulk delete middleware.
>
> A DELETE with the query parameters:
>
> ?multipart-manifest=delete&async=yes
> will schedule all the segments referenced in the manifest to be deleted asynchronously and then delete the manifest itself. Note that segments will continue to appear in listings and be counted for quotas until they are cleaned up by the object-expirer. This option is only available when all segments are in the same container and none of them are nested SLOs.

```mermaid
---
title: Sync object deletion
--- 
 %%{init: { 'sequence': {'noteAlign':'left'} } }%%
sequenceDiagram
    autonumber
    Participant s as SWIFT<br>Source
    Participant d as SWIFT<br>Destination
    Participant w as Chorus<br>Worker

    note over w: produced by<br>DELETE<br>object event
    w-)w: enqueue task
    activate w
    Note right of w: - acc ID<br>- container<br>- object<br>- [opt]VersionID<br>- DeleteMultipartManifest
    w->>+s: HEAD src object
    note right of s: X-Newest: true

    alt object not exists
      s->>-w: 404
      w->>+d: DELETE dest object
      d-x-w: 200 ok - exit
      deactivate w
    else object exists
      activate w
      s-xw: 200
      deactivate w
      note right of w: do nothig<br>object was recreated<br>after deletion<br>will be handled by<br>ObjectUpdate task
    end
```


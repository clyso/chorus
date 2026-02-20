# Docker-compose

## Before start

**Requirements:**
- Docker
- S3 client (e.g. [s3cmd](https://github.com/s3tools/s3cmd))
    ```shell
    brew install s3cmd
    ```
- Chorus management CLI [chorctl](../tools/chorctl)
    ```shell
    brew install clyso/tap/chorctl
    ```
**Structure:**
```
├── docker-compose
│   ├── docker-compose.yml    # docker-compose file
│   ├── FakeS3Dockerfile      # Dockerfile for in-memory S3 endpoint
│   ├── proxy-conf.yaml       # example config for chorus-proxy
│   ├── README.md
│   ├── s3cmd-follower.conf   # s3cmd credentials for follower storage
│   ├── s3cmd-main.conf       # s3cmd credentials for main storage
│   ├── s3cmd-proxy.conf      # s3cmd credentials for proxy storage
│   ├── s3-credentials.yaml   # chorus common config with S3 credentials
│   └── worker-conf.yaml      # example config for chorus-worker
```

Please also review [docker-compose.yml](./docker-compose.yml) file. It contains comments about services and their configuration.

## Bucket replication example

To run chorus with docker compose:
1. Clone repo:
    ```shell 
    git clone https://github.com/clyso/chorus.git && cd chorus
    ```
2. Start chorus `worker`,`proxy` with fake main and follower S3 backends:
    ```shell
    docker-compose -f ./docker-compose/docker-compose.yml --profile fake --profile proxy up
    ```
3. Check chorus config with CLI:
    ```
    % chorctl storage
    NAME            ADDRESS                          PROVIDER     USERS
    follower        http://fake-s3-follower:9000     Other        user1
    main [MAIN]     http://fake-s3-main:9000         Other        user1
    ```
    And check that there are no ongoing replications with `chorctl dash`.
4. Create a bucket named `test` in the `main` storage:
    ```shell
    s3cmd mb s3://test -c ./docker-compose/s3cmd-main.conf
    ```
    Check that main bucket is empty:
    ```shell
    s3cmd ls s3://test -c ./docker-compose/s3cmd-main.conf
    ```
5. Upload contents of this directory into `main`:
    ```shell
    s3cmd sync ./docker-compose/  s3://test -c ./docker-compose/s3cmd-main.conf
    ```
    Check that follower bucket is still not exists:
    ```shell
    s3cmd ls s3://test -c ./docker-compose/s3cmd-follower.conf
    ERROR: Bucket 'test' does not exist
    ERROR: S3 error: 404 (NoSuchBucket): The specified bucket does not exist
    ```
6. See `test` bucket available for replication:
    ```shell
    % chorctl repl buckets -u user1 -f main -t follower
    BUCKET
    test
    ```
7. Enable replication for the bucket:
    ```shell
    chorctl repl add -u user1 -b test -f main -t follower
    ```
    > **Tip**: For user-level replication (all existing and future buckets), omit the `-b` flag:
    > ```shell
    > chorctl repl add -u user1 -f main -t follower
    > ```
8. Check replication progress with:
    ```shell
    chorctl dash
    ```
9. Check that data is actually replicated to the follower:
    ```shell
    s3cmd ls s3://test -c ./docker-compose/s3cmd-follower.conf
    ```
10. Now do some live changes to bucket using proxy:
    ```shell
    s3cmd del s3://test/worker-conf.yaml -c ./docker-compose/s3cmd-proxy.conf
    ```
11. List main and follower contents again to see that the file was removed from both. Feel free to play around with storages using `s3cmd` and preconfigured configs [s3cmd-main.conf](./s3cmd-main.conf) [s3cmd-follower.conf](./s3cmd-follower.conf) [s3cmd-proxy.conf](./s3cmd-proxy.conf).

## Where to go next
Try to add more worker instances to speed up replication process and observe how they are balancing the load. To do this, duplicate existing `worker` service in [docker-compose.yml](./docker-compose.yml) with omitted ports to avoid conflicts:
```yaml
  worker2:
    depends_on:
      - redis
    build:
      context: ../
      args:
        SERVICE: worker
    volumes:
    - type: bind
      source: ./worker-conf.yaml
      target: /bin/config/config.yaml
    - type: bind
      source: ./s3-credentials.yaml
      target: /bin/config/override.yaml
```
The same can be done for `worker3`, `worker4`, etc. And for `proxy` service.

Replace S3 credentials in [./s3-credentials.yaml](./s3-credentials.yaml) with your own s3 storages and start docker-compose without fake backends:
```shell
docker-compose -f ./docker-compose/docker-compose.yml --profile proxy up
```

Explore more features with [chorctl](../tools/chorctl) (`chorctl --help`) and [WebUI](../ui).

## How-to
To tear-down:
```shell
docker-compose -f ./docker-compose/docker-compose.yml down
```
To tear down and wipe all replication metadata:
```
docker-compose -f ./docker-compose/docker-compose.yml down -v

```
Rebuild images with the latest source changes (`--build`):
```shell
docker-compose -f ./docker-compose/docker-compose.yml --profile fake --profile proxy --build up
```
To use chorus images from registry instead of building from source replace in [docker-compose.yml](./docker-compose.yml):
```yaml
  worker:
    build:
      context: ../
      args:
        SERVICE: worker
``` 
to:

```yaml
  worker:
    image: "harbor.clyso.com/chorus/worker:latest" 
```
And similar for `web-ui` and `proxy`.


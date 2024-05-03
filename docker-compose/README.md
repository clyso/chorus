# Docker-compose
**REQUIREMENTS:**
- Docker
- S3 client (e.g. [s3cmd](https://github.com/s3tools/s3cmd))
    ```shell
    brew install s3cmd
    ```
- Chorus management CLI [chorctl](../tools/chorctl)
    ```shell
    brew install clyso/tap/chorctl
    ```
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
5. See `test` bucket avaiable for replication:
    ```shell
    % chorctl repl buckets -u user1 -f main -t follower
    BUCKET
    test
    ```
6. Upload contents of this directory into `main`:
    ```shell
    s3cmd sync ./docker-compose/  s3://test -c ./docker-compose/s3cmd-main.conf
    ```
    Check that follower bucket is still not exists:
    ```shell
    s3cmd ls s3://test -c ./docker-compose/s3cmd-follower.conf
    ERROR: Bucket 'test' does not exist
    ERROR: S3 error: 404 (NoSuchBucket): The specified bucket does not exist
    ```
7. Enable replication for bucket:
    ```shell
    chorctl repl add -u user1 -b test -f main -t follower
    ```
    Check replication progress with `chorctl dash`
8. Check that data is synced to the follower:
    ```shell
    s3cmd ls s3://test -c ./docker-compose/s3cmd-follower.conf
    ```
9. Now do some live changes to bucket using proxy:
    ```shell
    s3cmd del s3://test/agent-conf.yaml -c ./docker-compose/s3cmd-proxy.conf
    ```
10. List main and follower contents again to see that the file was removed from both. Feel free to play around with storages using `s3cmd` and preconfigured configs [s3cmd-main.conf](./s3cmd-main.conf) [s3cmd-follower.conf](./s3cmd-follower.conf) [s3cmd-proxy.conf](./s3cmd-proxy.conf).

## Where to go next
Replace S3 credentials in [./s3-credentials.yaml](./s3-credentials.yaml) with your own s3 storages and start docker-compose without fake backends:
```shell
docker-compose -f ./docker-compose/docker-compose.yml --profile proxy up
```

Or try a setup with [chorus-agent](../service/agent) instead of proxy. Unlike `chorus-proxy`, `chorus-agent` dont't need to intercept s3 requests to propagate new cahges. Instead, it is capturing changes from [S3 bucket notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)
```shell
docker-compose -f ./docker-compose/docker-compose.yml --profile agent up
```

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
Use chorus images from registry instead of building from source replace in [docker-compose.yml](./docker-compose.yml):
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
And similar for `agent` and `proxy`.


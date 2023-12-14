# Chorus proxy

Serves s3 proxy on port `:9669`. Routes s3 requests to main s3 storage from [config](./config.yaml) and creates replication tasks for [worker](../worker) based on [replication policy](#usage).

## Usage

Set s3 storage and redis credentials to [config](./config.yaml) and run.

> [!IMPORTANT]  
> Both Proxy and [worker](../worker) should have the same Redis and s3 storages in config.

Manage replication policies with [CLI](../../tools/chorctl)
> [!NOTE]  
> [Worker](../worker) is required to use CLI. Deploy worker and provide worker GRPC api address to CLI.

With [CLI](../../tools/chorctl):
- check current replication state dashboard
  ```shell
  chorctl dash
  ```
- list buckets available for replication (remove `<` and `>` and use values from Proxy s3 storages config)
  ```shell
  chorctl repl buckets -f <from storage> -t <to storage> -u <user>
  ```
- create replciation for bucket:
  ```shell
  chorctl repl add -f <from storage> -t <to storage> -u <user> -b <bucket name>
  ```
- go to dashboard to check created replication progress
  ```shell
  chorctl dash
  ```

#### Auth config
Proxy supports only [s3 signature v4 Credentials](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html). V2 credentials can also be enabled in config.

Possible configurations:
- use credentials from other storages from config
    ```yaml
    ...
    auth:
      useStorage: storageOne
    ```
- use custom from config.
    ```yaml
    ...
    auth:
      custom: 
        user1:
          accessKeyID: <user1 v4 accessKey credential>
          secretAccessKey: <user1 v4 secretKey credential>
        user2:
          accessKeyID: <user2 v4 accessKey credential>
          secretAccessKey: <user2 v4 secretKey credential>
    ```
> [!IMPORTANT]  
> Custom credentials user keys (`user1` and `user2` in example above) should match user keys in storage config.
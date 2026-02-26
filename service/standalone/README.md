# Standalone version

Packs [proxy](../proxy), [worker](../worker), [embedded redis](https://github.com/alicebob/miniredis), and fake S3 endpoints into single binary.
Standalone version does not have persistence, so it cannot be used for production, but it allows to run chorus locally without dependencies.

## Usage

For installation options see main [README](../../README.md#installation).

Install standalone binary or run from source with `go run`:
```shell
% go run ./cmd/chorus
_________ .__
\_   ___ \|  |__   ___________ __ __  ______
/    \  \/|  |  \ /  _ \_  __ \  |  \/  ___/
\     \___|   Y  (  <_> )  | \/  |  /\___ \
 \______  /___|  /\____/|__|  |____//____  >
        \/     \/                        \/


S3 Proxy URL:   http://127.0.0.1:9669
S3 Proxy Credentials (AccessKey|SecretKey):
 - user1: [testKey1|testSecretKey1]
 - user2: [testKey2|testSecretKey2]

GRPC mgmt API:  127.0.0.1:9670
HTTP mgmt API:  http://127.0.0.1:9671
Redis URL:      127.0.0.1:33019

Storage list:
 - [FAKE] one: http://127.0.0.1:9680 < MAIN
 - [FAKE] two: http://127.0.0.1:9681
```

Output contains URLs and credentials for chorus services and fake S3 endpoints. With given credentials Chorus S3 Proxy or fake storages can be accessed with any S3 client. Example configuration for [s3cmd](https://github.com/s3tools/s3cmd):
```bash
cat << EOF > proxy.s3cmd
use_https = false
host_base = 127.0.0.1:9669
host_bucket = 127.0.0.1:9669
access_key = testKey1
secret_key = testSecretKey1
EOF
```
Create bucket with name `test` in Chorus S3 Proxy:
```bash
s3cmd mb s3://test -c proxy.s3cmd
```

To get full yaml config used by standalone binary run:
```shell
go run ./cmd/chorus print-config > chorus.yaml
```
Open `chorus.yaml` and check the configuration. Edit config to use your own S3 endpoints instead of fake ones or change other settings. Then run standalone binary with edited custom config:
```shell
go run ./cmd/chorus -config chorus.yaml
```

Install and run [chorctl CLI](../../tools/chorctl) to manage chorus services.


# Chorctl
CLI for chorus management api.

![chorctl.png](../../docs/media/chorctl.png)

## Install
```shell
go build .
```
Or download binary from [the latest release](https://github.com/clyso/chorus/releases).

Or with homebrew (MacOS and Linux only):
```shell
brew install clyso/tap/chorctl
```

## Usage

`chorctl` sends requests to GRPC api hosted by [Chorus worker](../../service/worker). 
Deploy worker and provide GRPC api address to `chorctl` with `--address` flag or `CHORUS_ADDRESS` envar, for example:
```shell
export CHORUS_ADDRESS=127.0.0.1:9670
```

```shell
 ./chorctl help
 
Chorctl is a CLI tool to monitor and manage chorus application
performing live migrations and replication of s3 storages.

Usage:
  chorctl [command]

Available Commands:
  agent       Prints information about registered notification agents
  check       Checks the files in the source and destination match.
  completion  Generate the autocompletion script for the specified shell
  dash        Open migration interactive dashboard
  help        Help about any command
  repl        list replications
  storage     Prints information about underlying chorus s3 storages

Flags:
  -a, --address string   address to chorus management grpc api (default: http://localhost:9670) (default "localhost:9670")
      --config string    config file (default is $HOME/.chorctl.yaml)
  -h, --help             help for chorctl
  -u, --user string      storage user
  -v, --verbose          prints additional log information

Use "chorctl [command] --help" for more information about a command.
```

Some useful commands:
- `chorctl dash` - shows live dashboard with bucket replication statuses
- `chorctl repl add -b <bucket name> -u <s3 user name from chorus config> -f <souce s3 storage from chorus config> -t <destination s3 storage from chorus config> `


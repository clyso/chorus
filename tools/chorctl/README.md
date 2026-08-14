# Chorctl

CLI for Chorus management API.

![chorctl.png](../../docs/media/chorctl.png)

## Install

From source (requires [Go language](https://go.dev/doc/install)):
```shell
cd ./tools/chorctl && go build .
```

Or install globally:
```shell
cd ./tools/chorctl && go install .
```

Or download binary from [the latest release](https://github.com/clyso/chorus/releases).

Or with homebrew (macOS and Linux only):
```shell
brew install clyso/tap/chorctl
```

## Usage

`chorctl` sends requests to the REST API hosted by [Chorus worker](../../service/worker).
Deploy worker and provide REST API URL (including `http://` or `https://` scheme) to `chorctl`
with `--address` flag or `CHORUS_ADDRESS` envar (default: `http://localhost:9671`):
```shell
export CHORUS_ADDRESS=http://127.0.0.1:9671
```

For `https://` addresses, TLS certificate verification can be disabled with `--insecure` (`-k`) flag.

If the [Web UI](../../ui) is deployed, it proxies the same API under `/api`. E.g. for UI on
`http://example.com:9090` use:
```shell
chorctl --address http://example.com:9090/api
```

Run `chorctl --help` for available commands. Key commands:

### Dashboard
```shell
chorctl dash
```

### Replications
```shell
# List replications
chorctl repl

# User-level replication (all buckets for user)
chorctl repl add -u <user> -f <from storage> -t <to storage>

# Bucket-level replication
chorctl repl add -u <user> -b <bucket> -f <from storage> -t <to storage>

# See all repl subcommands
chorctl repl --help
```

### Switch (change main storage after replication)
```shell
chorctl repl switch --help
```

### Routing
```shell
chorctl route --help
```

### Diff
```shell
chorctl diff --help
```

### Dynamic Credentials
```shell
chorctl set-user --help
```

For full command reference, run `chorctl <command> --help`.

#!/usr/bin/env bash
set -euo pipefail

# pinned plugin versions
PLUGIN_GO_VERSION="v1.36.2"
PLUGIN_GO_GRPC_VERSION="v1.5.1"
PLUGIN_GRPC_GATEWAY_VERSION="v2.25.1"
BUF_VERSION="v1.45.0"
PLUGIN_OPENAPIV2_VERSION="v2.15.2"

# determine script directory and root directory
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [[ "${SCRIPT_DIR##*/}" == "proto" ]]; then
  ROOT_DIR="$(dirname "$SCRIPT_DIR")"
  PROTO_DIR="$SCRIPT_DIR"
else
  ROOT_DIR="$SCRIPT_DIR"
  PROTO_DIR="$ROOT_DIR/proto"
fi

# create the build directory (.build) in the root to store the generated binaries
BUILD_DIR="$ROOT_DIR/.build"
BIN_DIR="$BUILD_DIR/bin"
mkdir -p "$BIN_DIR"

# export the binary path of the go plugins
GOBIN="$BIN_DIR"

# pinned plugin installation
echo "Installing pinned protoc-gen plugins ..."
GOBIN="$BIN_DIR" go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PLUGIN_GO_VERSION}"
GOBIN="$BIN_DIR" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PLUGIN_GO_GRPC_VERSION}"
GOBIN="$BIN_DIR" go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@${PLUGIN_GRPC_GATEWAY_VERSION}"
GOBIN="$BIN_DIR" go install "github.com/bufbuild/buf/cmd/buf@${BUF_VERSION}"
GOBIN="$BIN_DIR" go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@${PLUGIN_OPENAPIV2_VERSION}"

# run Buf generate
echo "Running buf generate ..."
(
  # changing the directory because there is a problem with the context which causes the buf to fail
  cd "$PROTO_DIR"  
  PATH="$BIN_DIR:$PATH" "$BIN_DIR/buf" generate --template "buf.gen.yaml" --config "buf.yaml"
)

echo "Proto generation complete."
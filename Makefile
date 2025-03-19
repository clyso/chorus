TOOLS_DIR="$(PWD)/.tools"

GIT_COMMIT=$(shell git log -1 --format=%H)
GIT_TAG=$(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
BUILD_DATE=$(shell date -Is -u)

GOIMPORTS_VERSION="0.29.0"
GOLANGCI_LINT_VERSION="1.64.8"
PROTOC_GEN_GO_VERSION="1.36.2"
PROTOC_GEN_GO_GRPC_VERSION="1.5.1"
PROTOC_GEN_GO_GRPC_GATEWAY_VERSION="2.25.1"
BUF_VERSION="1.45.0"
PROTOC_GEN_OPENAPIV2_VERSION="2.15.2"

.PHONY: all
all: agent chorus proxy worker chorctl bench

.PHONY: install-protobuf-tools
install-protobuf-tools:
	GOBIN=$(TOOLS_DIR) go install "google.golang.org/protobuf/cmd/protoc-gen-go@v$(PROTOC_GEN_GO_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@v$(PROTOC_GEN_GO_GRPC_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v$(PROTOC_GEN_GO_GRPC_GATEWAY_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v$(PROTOC_GEN_OPENAPIV2_VERSION)"

.PHONY: install-goimports-tools
install-goimports-tools:
	GOBIN=$(TOOLS_DIR) go install "golang.org/x/tools/cmd/goimports@v$(GOIMPORTS_VERSION)"

.PHONY: install-golangci-lint-tools
install-golangci-lint-tools:
	@if [ ! -f $(TOOLS_DIR)/golangci-lint ] || [ "$(shell $(TOOLS_DIR)/golangci-lint version | grep -c $(GOLANGCI_LINT_VERSION))" = 0 ]; then \
		 curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(TOOLS_DIR) v$(GOLANGCI_LINT_VERSION); \
	fi;
	
.PHONY: install-tools
install-tools: install-protobuf-tools install-goimports-tools install-golangci-lint-tools

.PHONY: tidy
tidy:
	go mod tidy
	cd tools/chorctl; go mod tidy
	cd tools/bench; go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: imports
imports:
	$(TOOLS_DIR)/goimports -local github.com/clyso/chorus -w ./cmd
	$(TOOLS_DIR)/goimports -local github.com/clyso/chorus -w ./pkg
	$(TOOLS_DIR)/goimports -local github.com/clyso/chorus -w ./service
	$(TOOLS_DIR)/goimports -local github.com/clyso/chorus -w ./tools

.PHONY: lint
lint:
	$(TOOLS_DIR)/golangci-lint run

.PHONY: pretty
pretty: tidy fmt vet imports lint

.PHONY: mkdir-build
mkdir-build: 
	mkdir -p build

# This target matches all targets ending with `-bin` and allows to execute 
# common goals only once per project, instead of once per every binary.
# Means, formatting and linting would be executed once for whole repository.
# `:` is a no-op operator in shell.
%-bin: install-tools pretty mkdir-build
	:

.PHONY: agent
agent: agent-bin
	go build -ldflags="-X 'main.date=$(BUILD_DATE)' -X 'main.version=$(GIT_TAG)' -X 'main.commit=$(GIT_COMMIT)'" -o build/agent cmd/agent/main.go

.PHONY: chorus
chorus: chorus-bin
	go build -ldflags="-X 'main.date=$(BUILD_DATE)' -X 'main.version=$(GIT_TAG)' -X 'main.commit=$(GIT_COMMIT)'" -o build/chorus cmd/chorus/main.go

.PHONY: proxy
proxy: proxy-bin
	go build -ldflags="-X 'main.date=$(BUILD_DATE)' -X 'main.version=$(GIT_TAG)' -X 'main.commit=$(GIT_COMMIT)'" -o build/proxy cmd/proxy/main.go

.PHONY: worker
worker: worker-bin
	go build -ldflags="-X 'main.date=$(BUILD_DATE)' -X 'main.version=$(GIT_TAG)' -X 'main.commit=$(GIT_COMMIT)'" -o build/worker cmd/worker/main.go

.PHONY: chorctl
chorctl: chorctl-bin
	cd tools/chorctl; go build -ldflags="-X 'main.date=$(BUILD_DATE)' -X 'main.version=$(GIT_TAG)' -X 'main.commit=$(GIT_COMMIT)'" -o ../../build/chorctl main.go

.PHONY: bench
bench: bench-bin
	cd tools/bench; go build -ldflags="-X 'main.date=$(BUILD_DATE)' -X 'main.version=$(GIT_TAG)' -X 'main.commit=$(GIT_COMMIT)'" -o ../../build/bench main.go

.PHONY: test
test: pretty
	go test ./...

.PHONY: proto-gen
proto-gen: install-protobuf-tools
	cd proto; PATH="$(TOOLS_DIR):$(PATH)" $(TOOLS_DIR)/buf generate --template "buf.gen.yaml" --config "buf.yaml"

.PHONY: clean
clean:
	rm -rf build/

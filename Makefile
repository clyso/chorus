TOOLS_DIR="$(PWD)/.tools"

GIT_COMMIT=$(shell git log -1 --format=%H)
GIT_TAG=$(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
BUILD_DATE=$(shell date -Ins)

GOLANGCI_LINT_VERSION="v1.63.4"
PROTOC_GEN_GO_VERSION="v1.36.2"
PROTOC_GEN_GO_GRPC_VERSION="v1.5.1"
PROTOC_GEN_GO_GRPC_GATEWAY_VERSION="v2.25.1"
BUF_VERSION="v1.45.0"
PROTOC_GEN_OPENAPIV2_VERSION="v2.15.2"

.PHONY: all
all: agent chorus proxy worker chorctl bench

.PHONY: install-tools
install-tools:
	mkdir -p $(TOOLS_DIR)
	GOBIN=$(TOOLS_DIR) go install golang.org/x/tools/cmd/goimports@latest
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(TOOLS_DIR) $(GOLANGCI_LINT_VERSION)
	GOBIN=$(TOOLS_DIR) go install "google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@$(PROTOC_GEN_GO_GRPC_GATEWAY_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)"
	GOBIN=$(TOOLS_DIR) go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@$(PROTOC_GEN_OPENAPIV2_VERSION)"

.PHONY: ensure-tools
ensure-tools:
	@if [[ ! -f $(TOOLS_DIR)/buf || ! -f $(TOOLS_DIR)/protoc-gen-go || ! -f $(TOOLS_DIR)/protoc-gen-go-grpc || ! -f $(TOOLS_DIR)/protoc-gen-grpc-gateway || ! -f $(TOOLS_DIR)/protoc-gen-openapiv2 || ! -f $(TOOLS_DIR)/goimports || ! -f $(TOOLS_DIR)/golangci-lint ]]; then echo 'Some tools are mising. Execute "make install-tools".' && exit 1; fi;

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

%-bin: ensure-tools pretty mkdir-build
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
proto-gen: ensure-tools
	cd proto; PATH="$(TOOLS_DIR):$(PATH)" $(TOOLS_DIR)/buf generate --template "buf.gen.yaml" --config "buf.yaml"

.PHONY: clean
clean:
	rm -rf build/

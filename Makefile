GIT_COMMIT=$(shell git log -1 --format=%H)
GIT_TAG=$(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
BUILD_DATE=$(shell date -Ins)

.PHONY: all
all: agent chorus proxy worker chorctl bench

.PHONY: install-tools
install-tools:
	go install golang.org/x/tools/cmd/goimports@latest
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.63.4

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
	goimports -local github.com/clyso/chorus -w ./

.PHONY: lint
lint:
	golangci-lint run

.PHONY: pretty
pretty: tidy fmt vet imports lint

.PHONY: mkdir-build
mkdir-build: 
	mkdir -p build

%-bin: pretty mkdir-build
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

.PHONY: clean
clean:
	rm -rf build/

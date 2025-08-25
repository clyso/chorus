GIT_COMMIT=$(shell git log -1 --format=%H)
GIT_TAG=$(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
BUILD_DATE=$(shell date -Is -u)

.PHONY: all
all: agent chorus proxy worker chorctl bench

.PHONY: tidy
tidy:
	go mod tidy
	cd tools/chorctl; go mod tidy
	cd tools/bench; go mod tidy

.PHONY: gen
gen:
	go generate ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: imports
imports:
	go tool goimports -local github.com/clyso/chorus -w ./cmd
	go tool goimports -local github.com/clyso/chorus -w ./pkg
	go tool goimports -local github.com/clyso/chorus -w ./service
	go tool goimports -local github.com/clyso/chorus -w ./tools

.PHONY: lint
lint:
	go tool golangci-lint run

.PHONY: vuln
vuln:
	go tool govulncheck ./...

.PHONY: license-check
license-check:
	find . -name "*.go" ! -name "*_test.go" | xargs go tool addlicense -check -c 'Clyso GmbH' || (echo "Missing license headers (run make license-fix):"; exit 1)

.PHONY: license-fix
license-fix:
	find . -name "*.go" ! -name "*_test.go" | xargs go tool addlicense -c 'Clyso GmbH' || (echo "Missing license headers"; exit 1)

.PHONY: pretty
pretty: tidy gen fmt vet imports lint vuln license-check

.PHONY: mkdir-build
mkdir-build: 
	mkdir -p build

# This target matches all targets ending with `-bin` and allows to execute 
# common goals only once per project, instead of once per every binary.
# Means, formatting and linting would be executed once for whole repository.
# `:` is a no-op operator in shell.
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

.PHONY: test
test: pretty
	go test ./...

.PHONY: proto-gen
proto-gen:
	cd proto; go tool buf generate --template "buf.gen.yaml" --config "buf.yaml"

.PHONY: clean
clean:
	rm -rf build/

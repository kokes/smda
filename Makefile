.PHONY: check run test bench coverstats build-docker run-docker test-docker bench-many dist

# call the makefile like `GORLS=gotip make test` to use an alternative Go release
GORLS ?= go

DIST_BUILD_OS ?= linux darwin windows
DIST_BUILD_ARCH ?= amd64 arm64

BUILD_OS = $(shell go env GOOS)
BUILD_ARCH = $(shell go env GOARCH)
BUILD_PATH = bin/smda-server
DIST_ARTIFACT = dist/smda-$(BUILD_OS)-$(BUILD_ARCH).zip
# make artifacts more understandable by not using 'darwin'
ifeq ($(BUILD_OS), darwin)
	DIST_ARTIFACT = dist/smda-macos-$(BUILD_ARCH).zip
endif

ifeq ($(BUILD_OS), windows)
	BUILD_PATH = bin/smda-server.exe
endif

DOCKER_IMAGE = smda
DOCKER_IMAGE_BUILD = smda-builder

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M)
BUILD_GO := $(shell $(GORLS) version)
BUILD_FLAGS = -ldflags "-X main.gitCommit=$(GIT_COMMIT) -X main.buildTime=$(BUILD_TIME) -X 'main.buildGoVersion=$(BUILD_GO)'"

check:
	$(GORLS) fmt ./...
	CGO_ENABLED=0 $(GORLS) vet ./...

build:
	mkdir -p bin
	CGO_ENABLED=0 $(GORLS) build -o $(BUILD_PATH) ${BUILD_FLAGS} ./cmd/server/

build-docker:
	docker build . -t $(DOCKER_IMAGE):latest

build-ingest:
	CGO_ENABLED=0 $(GORLS) build ./cmd/ingest/

run:
	$(GORLS) run cmd/server/main.go -port-http 8822 -samples -wdir tmp

run-tls:
	$(GORLS) run cmd/server/main.go -port-http 8822 -port-https 8823 -samples -wdir tmp -tls -tls-cert localhost.pem -tls-key localhost-key.pem

run-clean:
	mkdir -p tmp && rm -r tmp && make run

run-docker: build-docker
	# ephemeral run - will destroy the container after exiting
	docker run --rm -p 8822:8822 $(DOCKER_IMAGE):latest

# we need to inject GIT_COMMIT into the Docker image, because
# we don't have git nor the git repo there
# ARCH: consider making the docker build a separate step (or maybe even within `build-docker`)
dist: test
	docker build --target build -t $(DOCKER_IMAGE_BUILD) .
	@rm -rf dist
	mkdir dist
	@for os in $(DIST_BUILD_OS) ; do \
		for arch in $(DIST_BUILD_ARCH); do \
			echo "Buidling" $$arch $$os; \
			docker run --rm -v $(PWD)/dist:/smda/dist -e GOOS=$$os -e GOARCH=$$arch -e GIT_COMMIT=$(GIT_COMMIT) $(DOCKER_IMAGE_BUILD) make package; \
		done \
	done
	(cd dist; shasum -a 256 *.zip > sha256sums.txt)

package: build
	mkdir -p dist
	zip -j $(DIST_ARTIFACT) $(BUILD_PATH) LICENSE

# reset GOOS and GOARCH, because they may be set by an outside process
test:
	CGO_ENABLED=0 GOOS= GOARCH= $(GORLS) test -timeout 5s -coverprofile=coverage.out ./...

test-race:
	CGO_ENABLED=1 $(GORLS) test -race -timeout 5s -coverprofile=coverage.out ./...

test-docker:
	docker run --rm -v $(PWD):/smda golang:1.17-alpine sh -c "apk add --no-cache make && cd /smda && make test"

bench:
	GOMAXPROCS=1 CGO_ENABLED=0 $(GORLS) test -run=NONE -bench=. -benchmem ./...

bench-many:
	for i in {1..10}; do make bench; done | tee $(shell eval git rev-parse --abbrev-ref HEAD)_$(GORLS).txt

coverstats:
	CGO_ENABLED=0 $(GORLS) tool cover -func=coverage.out

pprof:
	CGO_ENABLED=0 $(GORLS) test -cpuprofile cpu.prof -memprofile mem.prof -bench=. ./src/
	$(GORLS) tool pprof src.test cpu.prof

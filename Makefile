.PHONY: check run test bench coverstats build-docker run-docker test-docker bench-many

# call the makefile like `GORLS=gotip make test` to use an alternative Go release
GORLS ?= go

check:
	$(GORLS) fmt ./...
	CGO_ENABLED=0 $(GORLS) vet ./...

build: check test
	CGO_ENABLED=0 $(GORLS) build ./cmd/server/

build-docker:
	docker build . -t kokes/smda:latest

build-ingest:
	CGO_ENABLED=0 $(GORLS) build ./cmd/ingest/

run:
	$(GORLS) run cmd/server/main.go -port 8822 -samples -wdir tmp

run-clean:
	mkdir -p tmp && rm -r tmp && make run

run-docker: build-docker
	# ephemeral run - will destroy the container after exiting
	docker run --rm -p 8822:8822 kokes/smda:latest

test:
	CGO_ENABLED=0 $(GORLS) test -timeout 5s -coverprofile=coverage.out ./...

test-docker:
	docker run --rm -v $(PWD):/smda golang:1.15-alpine sh -c "apk add --no-cache make && cd /smda && make test"

bench:
	GOMAXPROCS=1 CGO_ENABLED=0 $(GORLS) test -run=NONE -bench=. -benchmem ./...

bench-many:
	for i in {1..10}; do make bench; done | tee $(shell eval git rev-parse --abbrev-ref HEAD).txt

coverstats:
	CGO_ENABLED=0 $(GORLS) tool cover -func=coverage.out

pprof:
	CGO_ENABLED=0 $(GORLS) test -cpuprofile cpu.prof -memprofile mem.prof -bench=. ./src/
	$(GORLS) tool pprof src.test cpu.prof

.PHONY: build buildw run test bench testv coverstats

build:
	CGO_ENABLED=0 go build cmd/server.go

buildw:
	CGO_ENABLED=0 go build -ldflags -w cmd/server.go

build-docker:
	docker build . -t kokes/smda:latest

run:
	mkdir -p tmp && rm -r tmp && go run cmd/server.go -port 8822 -samples -wdir tmp

run-docker:
	# ephemeral run - will destroy the container after exiting
	docker run --rm -p 8822:8822 kokes/smda:latest

test:
	CGO_ENABLED=0 go test -timeout 5s -coverprofile=coverage.out ./...

test-docker:
	docker run --rm -v $(pwd):/smda golang:alpine sh -c "apk add --no-cache make && cd /smda && make test"

bench:
	CGO_ENABLED=0 go test -run=NONE -bench=. -benchmem ./...

testv:
	CGO_ENABLED=0 go test -test.v -timeout 5s -coverprofile=coverage.out ./...

coverstats:
	CGO_ENABLED=0 go tool cover -func=coverage.out

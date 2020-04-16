.PHONY: build buildw run test bench testv coverstats

build:
	go build cmd/server.go

buildw:
	go build -ldflags -w cmd/server.go

build-docker:
	docker build . -t kokes/smda:latest

run:
	mkdir -p tmp && rm -r tmp && go run cmd/server.go -port 8822 -samples -wdir tmp

run-docker:
	# ephemeral run - will destroy the container after exiting
	docker run --rm -p 8822:8822 kokes/smda:latest

test:
	go test -timeout 5s -coverprofile=coverage.out ./...

bench:
	go test -run=NONE -bench=. -benchmem ./...

testv:
	go test -test.v -timeout 5s -coverprofile=coverage.out ./...

coverstats:
	go tool cover -func=coverage.out

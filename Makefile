.PHONY: build buildw run test bench testv coverstats

build:
	go build cmd/server.go

buildw:
	go build -ldflags -w cmd/server.go

run:
	mkdir -p tmp && rm -r tmp && go run cmd/server.go -port 8822 -samples -wdir tmp

test:
	go test -timeout 5s -coverprofile=coverage.out ./...

bench:
	go test -bench=. -benchmem ./...

testv:
	go test -test.v -timeout 5s -coverprofile=coverage.out ./...

coverstats:
	go tool cover -func=coverage.out

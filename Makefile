build:
	go build cmd/server.go

buildw:
	go build -ldflags -w cmd/server.go

run:
	rm -r tmp && go run cmd/server.go -port 8822 -samples -wdir tmp

test:
	go test -timeout 5s -coverprofile=coverage.out ./...

testv:
	go test -test.v -timeout 5s -coverprofile=coverage.out ./...

coverstats:
	go tool cover -func=coverage.out

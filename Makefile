.PHONY: build test test-integration lint clean

BINARY_NAME=gomigrate

build:
	# go build -o .out/$(BINARY_NAME) ./cmd/gomigrate
	go build -ldflags="-s -w" -o .out/$(BINARY_NAME) ./cmd/gomigrate

test:
	go test -v ./internal/...

test-integration:
	go test -v ./test/integration/...

lint:
	golangci-lint run

clean:
	rm -f .out/$(BINARY_NAME)
	go clean

.PHONY: build test test-integration lint clean

BINARY_NAME=gomigrate

build:
	go build -o $(BINARY_NAME) ./cmd/gomigrate

test:
	go test -v ./internal/...

test-integration:
	go test -v ./test/integration/...

lint:
	golangci-lint run

clean:
	rm -f $(BINARY_NAME)
	go clean

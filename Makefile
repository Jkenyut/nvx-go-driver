.PHONY: all build test lint clean check

all: check build

build:
	go build -v ./...

test:
	go test -v -race -cover ./...

lint:
	@echo "Running golangci-lint..."
	golangci-lint run ./...

clean:
	go clean
	rm -f coverage.out

deps:
	go mod tidy
	go mod verify

check: deps lint test

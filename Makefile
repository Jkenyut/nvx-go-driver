.PHONY: all build test clean check

all: check build

build:
	go build -v ./...

test:
	go test -v -race -cover ./...

clean:
	go clean
	rm -f coverage.out

deps:
	go mod tidy
	go mod verify

check: deps test

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.PHONY: all


all: setup build test lint

setup:
	if [ ! -e $(shell go env GOPATH)/bin/golangci-lint ] ; then curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.31.0 ; fi;

build:
	go build ./...

dep:
	go mod tidy

lint:
	golangci-lint run --disable-all \
	--enable=staticcheck --enable=unused --enable=gosimple --enable=structcheck --enable=varcheck --enable=ineffassign \
	--enable=deadcode --enable=typecheck --enable=stylecheck --enable=gosec --enable=unconvert --enable=gofmt \
	--enable=unparam --enable=nakedret --enable=gochecknoinits --enable=depguard --enable=gocyclo --enable=misspell \
	--enable=megacheck --enable=goimports --enable=golint \
	--deadline=5m --no-config

lint-strict:
	golangci-lint run --enable-all

test:
	mkdir -p ./etc/out
	ENVIRONMENT=test go test -failfast -count 1 -timeout 30s -race -covermode=atomic -coverprofile=etc/out/profile.cov ./... && go tool cover -func=etc/out/profile.cov


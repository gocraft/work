FROM golang:1.14-alpine3.13

RUN apk --no-cache add git libc-dev gcc redis

RUN redis-server --daemonize yes

RUN sleep 10

RUN mkdir -p /go/src/github.com/gocraft/work
WORKDIR /go/src/github.com/gocraft/work

COPY go.mod go.sum .
RUN go mod download

COPY . .




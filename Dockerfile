FROM golang:1.19-alpine3.16


RUN mkdir -p /go/src/github.com/agschwender/gocraft-work
WORKDIR /go/src/github.com/agschwender/gocraft-work

# Copy over modules and preload them prior to copying the application
# code. This should download and build all dependencies which should
# speed up start up time in those cases where no modules have been
# changed.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV GO111MODULE=on
RUN go install -v ./cmd/...

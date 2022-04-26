run-redis:
	docker run --rm --name work-redis -p 6379:6379 -d redis:6.2.6

stop-redis:
	docker stop work-redis

run-tests:
	-go test -v -cover -race -p 1 ./...

test: run-redis run-tests stop-redis

.PHONY: run-redis stop-redis run-tests test

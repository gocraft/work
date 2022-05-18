.PHONY: test-setup
test-setup:
	@echo "+ $@"
	docker-compose up -d

.PHONY: test-teardown
test-teardown:
	@echo "+ $@"
	docker-compose rm -fsv

.PHONY: test-run
test-run:
	@echo "+ $@"
	go test -v -p 1 -race -coverprofile=coverage.txt -covermode=atomic ./...

.PHONY: test
test: test-setup test-run test-teardown

help:
	@echo "$$(grep -hE '^\S+:.*##' $(MAKEFILE_LIST) | sed -e 's/:.*##\s*/:/' | column -c2 -t -s :)"
.PHONY: help

test-setup: ## Prepare infrastructure for tests
	@echo "+ $@"
	docker-compose up -d
.PHONY: test-setup

test-teardown: ## Bring down test infrastructure
	@echo "+ $@"
	docker-compose rm -fsv
.PHONY: test-teardown

test-run: ## Run tests
	@echo "+ $@"
	go test -v -p 1 -race -coverprofile=coverage.txt -covermode=atomic ./...
.PHONY: test-run

test: test-setup test-run test-teardown ## Prepare infrastructure and run tests
.PHONY: test

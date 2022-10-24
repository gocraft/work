.PHONY: help build

default: help

build:
	@docker-compose build gocraft-work

build.test:
	@docker-compose build gocraft-work-test

shell: build
	@docker-compose run --rm gocraft-work /bin/sh

test: build.test
	@docker-compose run --rm gocraft-work-test

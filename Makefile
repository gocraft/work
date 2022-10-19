.PHONY: help build

default: help

build:
	@docker build -t gocraft-work .

shell:
	@docker run -it --rm --entrypoint /bin/sh gocraft-work

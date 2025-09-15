
all: clear lint test

repl:
	lein with-profile +test repl

release:
	lein release

.PHONY: test
test:
	lein test

lint:
	clj-kondo --lint src test

.PHONY: clear
clear:
	rm -rf target

docker-up: docker-down docker-rm
	docker compose up

docker-down:
	docker compose down --remove-orphans

docker-rm:
	docker compose rm --force

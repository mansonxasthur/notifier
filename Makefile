.PHONY: check test bench vet lint fmt-check hooks up down logs migrate-create

.DEFAULT_GOAL := check

# Everything to run before committing. Stops at the first failing step.
check: fmt-check vet lint test

# The postgres tests use testcontainers, so Docker must be running.
test:
	go test -race ./...

# Function speed on this machine, not capacity. Needs Docker. Save the output with the machine
# it ran on under docs/results/ (see docs/results/README.md).
bench:
	go test -run '^$$' -bench . -benchmem -benchtime 1s ./...

vet:
	go vet ./...

# Config in .golangci.yml. Install: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
lint:
	golangci-lint run ./...

# gofmt -l exits 0 even when it lists files, so fail on any output.
fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "unformatted:"; gofmt -l .; exit 1; }

# Run once per clone: git does not version .git/hooks, so point it at .githooks.
hooks:
	git config core.hooksPath .githooks

# Dev stack with live reload (compose picks up docker-compose.override.yml).
up:
	docker compose up -d --build

down:
	docker compose down

# Optional: make logs s=<service> (api, relay, sender, ...).
logs:
	docker compose logs -f $(s)

migrate-create:
ifndef  name
	$(error usage: make migrate-create name=<migration_name>)
endif
	migrate create -ext sql -dir migrations -format 20060102150405 $(name)

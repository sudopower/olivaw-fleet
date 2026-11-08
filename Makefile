TEST_DATABASE_URL ?= postgres://fleet:fleet@localhost:5433/fleet?sslmode=disable

.PHONY: up down test test-integration lint build

up: ## start Postgres and Mosquitto
	docker compose up -d --wait

down:
	docker compose down -v

build:
	go build -o bin/ ./cmd/...

test: ## unit tests, no dependencies
	go test -race ./...

test-integration: up ## unit + Postgres integration tests
	FLEET_TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 ./...

lint:
	gofmt -l . | (! grep .)
	go vet ./...

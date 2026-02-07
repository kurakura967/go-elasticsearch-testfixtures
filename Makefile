.PHONY: deps build lint test test-integration test-all es-up es-down es-wait clean

# Dependencies
deps:
	go mod download

# Build
build:
	go build ./...

# Lint
lint:
	golangci-lint run

# Unit tests
test:
	go test -v -race -coverprofile=coverage.out ./...

# Integration tests (requires Elasticsearch)
test-integration:
	go test -v -race -tags integration ./...

# All tests
test-all: test test-integration

# Elasticsearch
es-up:
	docker compose up -d

es-down:
	docker compose down

es-wait:
	@echo "Waiting for Elasticsearch..."
	@for i in $$(seq 1 30); do \
		if curl -fsSL http://localhost:9200/_cluster/health > /dev/null 2>&1; then \
			echo "Elasticsearch is ready"; \
			exit 0; \
		fi; \
		echo "Waiting... ($$i/30)"; \
		sleep 2; \
	done; \
	echo "Elasticsearch failed to start"; \
	exit 1

# Clean
clean:
	rm -f coverage.out
	docker compose down -v

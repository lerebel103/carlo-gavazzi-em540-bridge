# Variables
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
IMAGE_NAME = carlo-gavazzi-em540-bridge
DOCKER_USER = lerebel103

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build       - Build Docker image"
	@echo "  push        - Build & push multi-arch images (amd64 + arm64)"
	@echo "  up/start    - Start with docker-compose"
	@echo "  down/stop   - Stop with docker-compose"
	@echo "  logs        - View application logs"
	@echo "  test        - Run tests in parallel (-n auto)"
	@echo "  test-serial - Run all tests in serial"
	@echo "  lint        - Run linting checks"
	@echo "  format      - Format code"
	@echo "  clean       - Clean up Docker resources"
	@echo "  sync        - Install/sync all dependencies (including dev)"
	@echo "  lock        - Regenerate uv.lock"

.PHONY: build
build:
	@echo "Building Docker image (version: $(VERSION))..."
	docker build --build-arg VERSION=$(VERSION) -t $(DOCKER_USER)/$(IMAGE_NAME):latest .

.PHONY: push
push:
	@echo "Building and pushing multi-arch images (version: $(VERSION))..."
	docker buildx create --name multiarch --use --bootstrap 2>/dev/null || docker buildx use multiarch
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--tag $(DOCKER_USER)/$(IMAGE_NAME):latest \
		--tag $(DOCKER_USER)/$(IMAGE_NAME):$(VERSION) \
		--build-arg VERSION=$(VERSION) \
		--push \
		.

.PHONY: up start
up start:
	docker compose up -d --build

.PHONY: down stop
down stop:
	docker compose down

.PHONY: logs
logs:
	docker compose logs -f carlo-gavazzi-em540-bridge

.PHONY: sync
sync:
	# Intentionally not --frozen: allows local env to reconcile after pyproject.toml edits.
	# CI and Docker use --frozen for strict reproducibility.
	uv sync

.PHONY: lock
lock:
	uv lock

.PHONY: test
test:
	uv run pytest tests/ -v -n auto

.PHONY: test-serial
test-serial:
	uv run pytest tests/ -v

.PHONY: lint
lint:
	uv run ruff check app/ tests/
	uv run ruff format --check app/ tests/

.PHONY: format
format:
	uv run ruff format app/ tests/
	uv run ruff check --fix app/ tests/

.PHONY: clean
clean:
	docker compose down --rmi all --volumes --remove-orphans

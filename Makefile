# Variables
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
IMAGE_NAME = em540-bridge
DOCKER_USER = lerebel103

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build       - Build Docker image"
	@echo "  push        - Build & push multi-arch images (amd64 + arm64)"
	@echo "  up/start    - Start with docker-compose"
	@echo "  down/stop   - Stop with docker-compose"
	@echo "  logs        - View application logs"
	@echo "  test        - Run all tests"
	@echo "  lint        - Run linting checks"
	@echo "  format      - Format code"
	@echo "  clean       - Clean up Docker resources"

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
	docker compose logs -f em540-bridge

.PHONY: test
test:
	python -m pytest tests/ -v

.PHONY: lint
lint:
	python -m ruff check app/ tests/
	python -m ruff format --check app/ tests/

.PHONY: format
format:
	python -m ruff format app/ tests/
	python -m ruff check --fix app/ tests/

.PHONY: clean
clean:
	docker compose down --rmi all --volumes --remove-orphans

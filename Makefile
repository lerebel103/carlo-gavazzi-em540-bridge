# Variables
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
IMAGE_NAME = carlo-gavazzi-em540-bridge
DOCKER_USER = lerebel103
PYTHON ?= python3
PYTEST_XDIST_AVAILABLE := $(shell $(PYTHON) -c "import importlib.util,sys; sys.stdout.write('1' if importlib.util.find_spec('xdist') else '0')")

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build       - Build Docker image"
	@echo "  push        - Build & push multi-arch images (amd64 + arm64)"
	@echo "  up/start    - Start with docker-compose"
	@echo "  down/stop   - Stop with docker-compose"
	@echo "  logs        - View application logs"
	@echo "  test        - Run tests (parallel when pytest-xdist is available)"
	@echo "  test-serial - Run all tests in serial"
	@echo "  test-parallel - Run all tests in parallel (-n auto)"
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
	docker compose logs -f carlo-gavazzi-em540-bridge

.PHONY: test
test:
	@if [ "$(PYTEST_XDIST_AVAILABLE)" = "1" ]; then \
		echo "Running tests in parallel (-n auto)"; \
		$(PYTHON) -m pytest tests/ -v -n auto; \
	else \
		echo "pytest-xdist not installed; running tests in serial"; \
		$(PYTHON) -m pytest tests/ -v; \
	fi

.PHONY: test-serial
test-serial:
	$(PYTHON) -m pytest tests/ -v

.PHONY: test-parallel
test-parallel:
	$(PYTHON) -m pytest tests/ -v -n auto

.PHONY: lint
lint:
	$(PYTHON) -m ruff check app/ tests/
	$(PYTHON) -m ruff format --check app/ tests/

.PHONY: format
format:
	$(PYTHON) -m ruff format app/ tests/
	$(PYTHON) -m ruff check --fix app/ tests/

.PHONY: clean
clean:
	docker compose down --rmi all --volumes --remove-orphans

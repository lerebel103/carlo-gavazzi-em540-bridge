
build:
	docker build -t lerebel103/em540-bridge:latest .
	docker compose build

run:
	docker compose up --build

run-service:
	docker compose up -d --build

# Linting
lint:
	flake8 .
	black --check .
	isort --check-only .

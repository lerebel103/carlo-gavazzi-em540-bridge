
build:
	docker build -t lerebel103/em540-bridge:latest .
	docker compose build

run:
	docker compose up --build

run-service:
	docker compose up -d --build

test:
	python -m unittest discover -p '*_test.py'

# Linting
lint:
	black .
	isort .
	flake8 --config .flake8 .

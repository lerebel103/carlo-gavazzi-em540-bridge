
build:
	docker build -t lerebel103/em540-bridge:latest .
	docker compose build

run:
	docker compose up --build

run-service:
	docker compose up -d --build


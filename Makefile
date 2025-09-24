
build:
	docker build -t lerebel103/em540-bridge:latest .

run:
	docker run -it --name em540-bridge \
		--rm \
		lerebel103/em540-bridge:latest

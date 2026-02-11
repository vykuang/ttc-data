include .env
build:
	docker buildx build -t ttc-data:${ENV} .
airu:
	docker compose -f compose.yml up -d
aird:
	docker compose down

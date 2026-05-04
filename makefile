include .env

IMAGE_NAME ?= ttc-data
ENV ?= dev

build:
	docker buildx build \
	-t $(IMAGE_NAME):${ENV} .
airflow-celery:
	docker compose -f compose.yml up -d
airflow:
	docker compose -f compose.local.yml up -d
aird:
	docker compose down
test:
	pytest
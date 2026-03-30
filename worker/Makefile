.PHONY: help test lint build push

.DEFAULT_GOAL := help

IMAGE := localhost:32000/markets-o11y-worker:latest

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

test: ## Run tests
	pytest

lint: ## Run linter
	ruff check app/

build: ## Build Docker image
	docker build -t $(IMAGE) .

push: build ## Build and push to microk8s registry
	docker push $(IMAGE)

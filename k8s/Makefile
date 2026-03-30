# markets-o11y Kubernetes Deployment Makefile

.PHONY: help apply-prod apply-secrets-prod diff-prod delete-prod watch-prod status-prod deploy-worker-prod logs-worker

.DEFAULT_GOAL := help

IMAGE := ghcr.io/atas/markets-o11y-worker
TAG   ?= sha-$(shell git -C .. rev-parse --short HEAD)

help: ## Show this help message
	@echo "markets-o11y Kubernetes Deployment"
	@echo ""
	@echo "Usage: make [target] [TAG=sha-abc1234]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

apply-prod: ## Apply all manifests with worker image tag (usage: make apply-prod [TAG=sha-abc1234])
	@echo "Applying to production with $(IMAGE):$(TAG)..."
	kubectl kustomize kustomize/overlays/prod | sed 's|image: $(IMAGE):.*|image: $(IMAGE):$(TAG)|' | kubectl apply -f -
	@echo "Done! Check pods with: kubectl get pods -n markets-o11y-prod"

apply-secrets-prod: ## Apply secrets to production (manual, not in kustomize)
	@if [ ! -f .env.prod ]; then \
		echo "Error: .env.prod not found!"; \
		echo "Copy .env.prod.template to .env.prod and fill in values"; \
		exit 1; \
	fi
	@echo "Generating and applying secrets to production..."
	@kubectl create namespace markets-o11y-prod --dry-run=client -o yaml | kubectl apply -f -
	@set -a && . .env.prod && set +a && \
		envsubst < manual/prod/secrets.yaml.tpl | kubectl apply -n markets-o11y-prod -f -
	@echo "Secrets applied to markets-o11y-prod namespace."

diff-prod: ## Show diff before applying to production
	kubectl diff -k kustomize/overlays/prod || true

delete-prod: ## Delete all production resources
	kubectl delete -k kustomize/overlays/prod

watch-prod: ## Watch production pods in real-time
	kubectl get pods -n markets-o11y-prod -w

status-prod: ## Show status of all production resources
	@echo "=== Pods ==="
	kubectl get pods -n markets-o11y-prod
	@echo "\n=== Services ==="
	kubectl get svc -n markets-o11y-prod
	@echo "\n=== PVCs ==="
	kubectl get pvc -n markets-o11y-prod

deploy-worker-prod: ## Deploy worker with git SHA tag (usage: make deploy-worker-prod [TAG=sha-abc1234])
	@echo "Deploying $(IMAGE):$(TAG)"
	kubectl set image deployment/worker worker=$(IMAGE):$(TAG) -n markets-o11y-prod
	kubectl rollout status deployment/worker -n markets-o11y-prod

logs-worker: ## Tail worker logs
	kubectl logs -f deployment/worker -n markets-o11y-prod

# markets-o11y Kubernetes Deployment Makefile

.PHONY: help apply-prod apply-secrets-prod diff-prod delete-prod watch-prod status-prod build-worker deploy-worker-prod logs-worker

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "markets-o11y Kubernetes Deployment"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

apply-prod: ## Apply manifests to production cluster
	@echo "Applying to production..."
	kubectl apply -k kustomize/overlays/prod
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

build-worker: ## Build worker image and load into microk8s
	docker build -t localhost:32000/markets-o11y-worker:latest ../worker
	docker push localhost:32000/markets-o11y-worker:latest

deploy-worker-prod: build-worker ## Build, push, and restart worker
	kubectl rollout restart deployment/worker -n markets-o11y-prod
	kubectl rollout status deployment/worker -n markets-o11y-prod

logs-worker: ## Tail worker logs
	kubectl logs -f deployment/worker -n markets-o11y-prod

# markets-o11y Kubernetes Deployment Makefile

.PHONY: help apply-prod apply-secrets-prod diff-prod delete-prod watch-prod status-prod logs-worker

.DEFAULT_GOAL := help

# The worker runs ghcr.io/atas/markets-o11y-worker:main; Keel (atas/microk8s k8s/keel.md)
# rolls it to every new :main digest, so no target here sets an image.

help: ## Show this help message
	@echo "markets-o11y Kubernetes Deployment"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

apply-prod: ## Apply all manifests (the worker's build stays whatever :main is)
	kubectl kustomize --load-restrictor LoadRestrictionsNone kustomize/overlays/prod | kubectl apply -f -
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
	kubectl kustomize --load-restrictor LoadRestrictionsNone kustomize/overlays/prod | kubectl diff -f - || true

delete-prod: ## Delete all production resources
	kubectl kustomize --load-restrictor LoadRestrictionsNone kustomize/overlays/prod | kubectl delete -f -

watch-prod: ## Watch production pods in real-time
	kubectl get pods -n markets-o11y-prod -w

status-prod: ## Show status of all production resources
	@echo "=== Pods ==="
	kubectl get pods -n markets-o11y-prod
	@echo "\n=== Services ==="
	kubectl get svc -n markets-o11y-prod
	@echo "\n=== PVCs ==="
	kubectl get pvc -n markets-o11y-prod

logs-worker: ## Tail worker logs
	kubectl logs -f deployment/worker -n markets-o11y-prod

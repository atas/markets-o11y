apiVersion: v1
kind: Secret
metadata:
  name: markets-o11y-secrets
  namespace: markets-o11y-prod
  labels:
    app.kubernetes.io/name: markets-o11y
type: Opaque
stringData:
  # Database
  POSTGRES_USER: "${POSTGRES_USER}"
  POSTGRES_PASSWORD: "${POSTGRES_PASSWORD}"
  POSTGRES_DB: "${POSTGRES_DB}"

  # Grafana
  GF_SECURITY_ADMIN_PASSWORD: "${GF_SECURITY_ADMIN_PASSWORD}"

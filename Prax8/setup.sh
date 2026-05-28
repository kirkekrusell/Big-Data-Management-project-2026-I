#!/usr/bin/env bash
# One-shot bring-up for Maria's Reliability Lab.
# Copies .env.example -> .env if missing, pulls + builds images, starts the stack,
# and runs make verify-stack at the end.
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "[setup] Created .env from .env.example"
fi

mkdir -p alerts

echo "[setup] Pulling base images (first run may take several minutes)..."
docker compose pull --ignore-pull-failures

echo "[setup] Building custom images (airflow, spark)..."
docker compose build

echo "[setup] Starting services..."
docker compose up -d

echo "[setup] Waiting ~20s for services to settle..."
sleep 20

if make verify-stack; then
  echo ""
  echo "[setup] 💚 Stack is up. Airflow UI: http://localhost:8080 (login: airflow / airflow)"
else
  echo ""
  echo "[setup] ⚠️  Some services are not yet healthy."
  echo "        Wait a minute and run 'make verify-stack' again."
  echo "        If still red, run 'make logs' to inspect."
  exit 1
fi

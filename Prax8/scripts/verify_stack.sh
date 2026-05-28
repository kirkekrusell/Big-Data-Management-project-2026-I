#!/usr/bin/env bash
# Pings every service in the compose stack and reports green/red.
# Designed for the §6.1 "make verify-stack" output format.
set -u

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
RESET="\033[0m"

ERR=0

ok()   { printf "${GREEN}✅ %s${RESET}\n" "$1"; }
warn() { printf "${YELLOW}⚠️  %s${RESET}\n" "$1"; }
fail() { printf "${RED}❌ %s${RESET}\n" "$1"; ERR=1; }

# --- postgres ---------------------------------------------------------------
if docker compose exec -T postgres pg_isready -U airflow >/dev/null 2>&1; then
  ok "postgres        — accepting connections"
else
  fail "postgres        — not ready (try: docker compose logs postgres)"
fi

# --- catalog (Hive Metastore) -----------------------------------------------
if docker compose exec -T catalog bash -c '</dev/tcp/localhost/9083' >/dev/null 2>&1; then
  ok "catalog         — Hive Metastore thrift listener up (port 9083)"
else
  fail "catalog         — Hive Metastore thrift not reachable (try: docker compose logs catalog)"
fi

# --- storage (MinIO) --------------------------------------------------------
if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
  if docker compose exec -T storage sh -c 'test -d /data/warehouse' >/dev/null 2>&1; then
    ok "storage         — MinIO healthy, warehouse bucket exists"
  else
    fail "storage         — MinIO up but warehouse bucket missing (try: docker compose up -d mc)"
  fi
else
  fail "storage         — MinIO not healthy (try: docker compose logs storage)"
fi

# --- airflow webserver ------------------------------------------------------
if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
  ok "airflow-web     — healthy (http 200)"
else
  fail "airflow-web     — not healthy (try: docker compose logs airflow-webserver)"
fi

# --- airflow scheduler ------------------------------------------------------
if docker compose exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob --limit 1 >/dev/null 2>&1; then
  ok "airflow-sched   — heartbeat fresh"
else
  fail "airflow-sched   — no recent heartbeat (try: docker compose logs airflow-scheduler)"
fi

# --- spark ------------------------------------------------------------------
if curl -fsS http://localhost:8081 >/dev/null 2>&1; then
  # Try to confirm at least one worker is registered (best-effort).
  if curl -fsS http://localhost:8081/json/ 2>/dev/null | python3 -c "import sys,json;sys.exit(0 if json.load(sys.stdin).get('aliveworkers',0)>=1 else 1)" 2>/dev/null; then
    ok "spark           — master alive, executor registered"
  else
    warn "spark           — master alive (no workers registered yet — may take ~10s)"
  fi
else
  fail "spark           — master UI not reachable (try: docker compose logs spark)"
fi

# --- trino ------------------------------------------------------------------
if curl -fsS http://localhost:8082/v1/info >/dev/null 2>&1; then
  if docker compose exec -T trino trino --execute "SHOW CATALOGS" 2>/dev/null | grep -q iceberg_datalake; then
    ok "trino           — coordinator healthy, iceberg_datalake catalog registered"
  else
    warn "trino           — coordinator reachable (catalog check pending)"
  fi
else
  fail "trino           — coordinator not reachable (try: docker compose logs trino)"
fi

echo ""
if [ "$ERR" = "0" ]; then
  printf "${GREEN}All engines green. You're ready for the workshop. 💚${RESET}\n"
else
  printf "${RED}One or more services are unhealthy. Investigate above before continuing.${RESET}\n"
  exit 1
fi

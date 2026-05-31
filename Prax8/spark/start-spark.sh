#!/usr/bin/env bash
# Single-container Spark standalone master + worker. The master runs in the
# background; the worker runs in the foreground so docker treats it as the main
# process (and tears the container down if it dies).
#
# We invoke spark-class directly instead of the bitnami start-master.sh wrapper
# because that wrapper tails the daemon log file and never returns, so the
# worker would never start.
set -euo pipefail

export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-0.0.0.0}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-8081}
export SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:-8082}

mkdir -p /opt/bitnami/spark/logs /opt/bitnami/spark/work

/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host "${SPARK_MASTER_HOST}" \
  --port "${SPARK_MASTER_PORT}" \
  --webui-port "${SPARK_MASTER_WEBUI_PORT}" &
MASTER_PID=$!

# Wait for the master to bind its RPC port before launching the worker.
for i in $(seq 1 30); do
  if (echo >/dev/tcp/localhost/${SPARK_MASTER_PORT}) 2>/dev/null; then
    break
  fi
  sleep 1
done

exec /opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
  "spark://localhost:${SPARK_MASTER_PORT}"

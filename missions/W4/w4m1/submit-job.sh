#!/bin/bash

set -e

CONTAINER_NAME="spark-master"
MASTER_URL="spark://spark-master:7077"
APP_PATH="/opt/spark-apps/pi.py"

echo "Submitting Spark job..."
echo "Container : ${CONTAINER_NAME}"
echo "Master    : ${MASTER_URL}"
echo "App       : ${APP_PATH}"

docker exec "${CONTAINER_NAME}" \
  spark-submit \
  --master "${MASTER_URL}" \
  "${APP_PATH}"
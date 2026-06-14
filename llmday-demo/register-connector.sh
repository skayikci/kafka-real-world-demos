#!/bin/bash
set -e
# Waits for Debezium Connect to become ready, then registers the PostgreSQL connector.

TIMEOUT=120
ELAPSED=0

echo "Waiting for Kafka Connect to be ready..."
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
  if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
    echo "Error: Kafka Connect did not become ready within ${TIMEOUT}s."
    exit 1
  fi
  sleep 2
  ELAPSED=$((ELAPSED + 2))
done
echo "Kafka Connect is ready."

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "demo",
      "database.password": "demo",
      "database.dbname": "demo",
      "topic.prefix": "demo",
      "table.include.list": "public.products",
      "plugin.name": "pgoutput"
    }
  }')

if [ "$HTTP_STATUS" = "201" ]; then
  echo "Connector registered."
elif [ "$HTTP_STATUS" = "409" ]; then
  echo "Connector already registered — reusing existing."
else
  echo "Error: connector registration returned HTTP $HTTP_STATUS."
  exit 1
fi

echo "  Status: curl http://localhost:8083/connectors/postgres-connector/status"

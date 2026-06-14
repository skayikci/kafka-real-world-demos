#!/bin/bash
set -e

echo "Starting stack..."
docker compose up -d

echo "Waiting for Kafka..."
until docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
  sleep 2
done

echo "Creating topics..."
docker compose exec -T kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic image-events-chaos \
  --partitions 1 --replication-factor 1

docker compose exec -T kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic image-events \
  --partitions 1 --replication-factor 1

./register-schema.sh

echo ""
echo "Stack ready."
echo "  Kafka UI:        http://localhost:8080"
echo "  Schema Registry: http://localhost:8081/subjects"
echo ""
echo "Demo sequence:"
echo "  Step 1 — The old world:     python chaos_producer.py"
echo "  Step 2 — The broken read:   python chaos_consumer.py"
echo "  Step 3 — The fix:           python producer.py"
echo "  Step 4 — Clean reads:       python consumer.py"
echo "  Step 5 — Try to break it:   python break_schema.py"

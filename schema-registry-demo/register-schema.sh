#!/bin/bash
set -e

SCHEMA_REGISTRY_URL="http://localhost:8081"
TOPIC="image-events"

echo "Waiting for Schema Registry..."
until curl -sf "$SCHEMA_REGISTRY_URL/subjects" > /dev/null; do
  sleep 2
done

echo "Registering schema for topic: $TOPIC"

SCHEMA_JSON=$(python3 -c "import json,sys; print(json.dumps({'schema': open('schemas/image-event.avsc').read()}))")

curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "$SCHEMA_JSON" \
  "$SCHEMA_REGISTRY_URL/subjects/${TOPIC}-value/versions"

echo ""
echo "Setting BACKWARD compatibility..."

curl -s -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}' \
  "$SCHEMA_REGISTRY_URL/config/${TOPIC}-value"

echo ""
echo "✓ Schema registered. Compatibility: BACKWARD"

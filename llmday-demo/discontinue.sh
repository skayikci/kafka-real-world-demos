#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: ./discontinue.sh \"Product Name\""
  exit 1
fi

# Escape single quotes for safe SQL interpolation ('' is the standard PostgreSQL escape)
SAFE_NAME="${1//\'/\'\'}"

docker exec llmday-demo-postgres-1 psql -U demo -d demo \
  -c "UPDATE products SET status = 'discontinued' WHERE name = '${SAFE_NAME}' RETURNING name, status;"

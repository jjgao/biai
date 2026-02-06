#!/bin/bash
set -e

echo "================================================"
echo "BIAI - Stopping Application"
echo "================================================"

# Navigate to project root
cd "$(dirname "$0")/.."

# Use .env.production if it exists
ENV_FILE_ARG=""
if [ -f .env.production ]; then
    ENV_FILE_ARG="--env-file .env.production"
fi

echo ""
echo "Stopping containers..."
docker compose -f docker-compose.prod.yml $ENV_FILE_ARG down

echo ""
echo "================================================"
echo "Application stopped!"
echo "================================================"

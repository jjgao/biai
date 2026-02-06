#!/bin/bash
set -e

echo "================================================"
echo "BIAI - Building Docker Images"
echo "================================================"

# Navigate to project root
cd "$(dirname "$0")/.."

# Use .env.production if it exists
ENV_FILE_ARG=""
if [ -f .env.production ]; then
    ENV_FILE_ARG="--env-file .env.production"
fi

echo ""
echo "Building images..."
docker compose -f docker-compose.prod.yml $ENV_FILE_ARG build

echo ""
echo "================================================"
echo "Build complete!"
echo "================================================"
echo ""
echo "To deploy, run: ./scripts/deploy.sh"

#!/bin/bash
set -e

echo "================================================"
echo "BIAI - Building Docker Images"
echo "================================================"

# Navigate to project root
cd "$(dirname "$0")/.."

echo ""
echo "Building images..."
docker compose -f docker-compose.prod.yml build

echo ""
echo "================================================"
echo "Build complete!"
echo "================================================"
echo ""
echo "To deploy, run: ./scripts/deploy.sh"

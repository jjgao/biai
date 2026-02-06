#!/bin/bash
set -e

echo "================================================"
echo "BIAI - Stopping Application"
echo "================================================"

# Navigate to project root
cd "$(dirname "$0")/.."

echo ""
echo "Stopping containers..."
docker compose -f docker-compose.prod.yml down

echo ""
echo "================================================"
echo "Application stopped!"
echo "================================================"

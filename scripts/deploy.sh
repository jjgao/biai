#!/bin/bash
set -e

echo "================================================"
echo "BIAI - Deploying Application"
echo "================================================"

# Navigate to project root
cd "$(dirname "$0")/.."

# Check if images need to be built
if [ "$1" = "--build" ]; then
    echo ""
    echo "Building images..."
    docker compose -f docker-compose.prod.yml build
fi

echo ""
echo "Stopping existing containers..."
docker compose -f docker-compose.prod.yml down 2>/dev/null || true

echo ""
echo "Starting containers..."
docker compose -f docker-compose.prod.yml up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 15

echo ""
echo "Container status:"
docker compose -f docker-compose.prod.yml ps

echo ""
echo "================================================"
echo "Deployment complete!"
echo "================================================"
echo ""
echo "Application is available at: http://localhost"
echo ""
echo "Useful commands:"
echo "  View logs:    docker compose -f docker-compose.prod.yml logs -f"
echo "  Stop:         docker compose -f docker-compose.prod.yml down"
echo "  Restart:      docker compose -f docker-compose.prod.yml restart"

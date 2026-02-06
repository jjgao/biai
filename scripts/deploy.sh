#!/bin/bash
set -e

echo "================================================"
echo "BIAI - Deploying Application"
echo "================================================"

# Navigate to project root
cd "$(dirname "$0")/.."

# Create .env.production if it doesn't exist
if [ ! -f .env.production ]; then
    echo ""
    echo "Creating .env.production from example..."
    cp .env.production.example .env.production
    echo "WARNING: Using default credentials. Edit .env.production for production use!"
fi

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
echo "Running health checks..."

# Wait for containers to start
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    attempt=$((attempt + 1))
    
    # Check if all containers are healthy
    unhealthy=$(docker compose -f docker-compose.prod.yml ps --format json 2>/dev/null | grep -c '"Health":"starting"' || true)
    
    if [ "$unhealthy" = "0" ]; then
        # All containers are either healthy or have no health check
        break
    fi
    
    echo "  Waiting for services to be healthy... ($attempt/$max_attempts)"
    sleep 2
done

# Verify health endpoints
echo ""
echo "Smoke tests:"

# Test client health
if curl -sf http://localhost/health > /dev/null 2>&1; then
    echo "  ✓ Client health check passed"
else
    echo "  ✗ Client health check failed"
    docker compose -f docker-compose.prod.yml logs client --tail=20
    exit 1
fi

# Test API proxy
if curl -sf http://localhost/api/datasets > /dev/null 2>&1; then
    echo "  ✓ API proxy check passed"
else
    echo "  ✗ API proxy check failed"
    docker compose -f docker-compose.prod.yml logs server --tail=20
    exit 1
fi

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

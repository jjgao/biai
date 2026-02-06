# Docker Deployment Guide

This guide explains how to deploy the BIAI application using Docker containers.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose v2.0+
- At least 4GB RAM available
- Port 80 available on the host

## Quick Start

### 1. Build and Deploy

```bash
# Build Docker images
./scripts/build.sh

# Deploy the application
./scripts/deploy.sh
```

### 2. Access the Application

Open http://localhost in your browser.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Network                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Client    │  │   Server    │  │     ClickHouse      │  │
│  │   (nginx)   │──│   (Node)    │──│     (Database)      │  │
│  │   :80       │  │   :5001     │  │     :8123           │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
    Host Port 80
```

## Container Details

### Client (nginx)
- Serves the React frontend
- Proxies `/api/*` requests to the server
- Health check endpoint: `/health`

### Server (Node.js)
- Express.js API server
- Connects to ClickHouse database
- Health check endpoint: `/health`

### ClickHouse
- Column-oriented OLAP database
- Data persisted in Docker volume
- Initialized with scripts from `./clickhouse/init/`

## Commands

### Build Images
```bash
./scripts/build.sh
```

### Deploy Application
```bash
./scripts/deploy.sh

# Or with fresh build
./scripts/deploy.sh --build
```

### Stop Application
```bash
./scripts/stop.sh
```

### View Logs
```bash
# All services
docker compose -f docker-compose.prod.yml logs -f

# Specific service
docker compose -f docker-compose.prod.yml logs -f server
docker compose -f docker-compose.prod.yml logs -f client
docker compose -f docker-compose.prod.yml logs -f clickhouse
```

### Check Status
```bash
docker compose -f docker-compose.prod.yml ps
```

### Restart Services
```bash
docker compose -f docker-compose.prod.yml restart
```

## Environment Configuration

Create a `.env.production` file based on `.env.production.example`:

```bash
cp .env.production.example .env.production
# Edit .env.production with your values
```

## Volumes

| Volume | Purpose |
|--------|---------|
| `clickhouse_data` | ClickHouse database files |

### Backup ClickHouse Data
```bash
docker run --rm -v biai_clickhouse_data:/data -v $(pwd):/backup alpine \
  tar czf /backup/clickhouse-backup.tar.gz /data
```

## Production Considerations

### Security
- Use specific image versions (not `latest`)
- Configure TLS/HTTPS (use a reverse proxy like Traefik or Caddy)
- Set up proper secrets management
- Use Docker secrets for sensitive data

### Monitoring
- Container health checks are configured
- Integrate with monitoring solutions (Prometheus, Grafana)
- Set up log aggregation (ELK, Loki)

### Scaling
- For high availability, consider Kubernetes
- Database can be scaled separately
- Client and server can be replicated behind a load balancer

## Troubleshooting

### Container won't start
```bash
docker compose -f docker-compose.prod.yml logs <service>
```

### Database connection issues
Check if ClickHouse is healthy:
```bash
docker compose -f docker-compose.prod.yml exec clickhouse clickhouse-client --query "SELECT 1"
```

### Port already in use
```bash
# Find process using port 80
lsof -i :80

# Use a different port
# Edit docker-compose.prod.yml, change "80:80" to "8080:80"
```

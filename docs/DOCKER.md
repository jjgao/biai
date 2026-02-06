# Docker Deployment Guide

This guide explains how to deploy the BIAI application using Docker containers.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose v2.0+
- At least 4GB RAM available
- Port 80 available on the host

## Quick Start

### 1. Configure Environment

```bash
# Create production config from example
cp .env.production.example .env.production

# Edit with your values (especially passwords!)
nano .env.production
```

### 2. Build and Deploy

```bash
# Build Docker images
./scripts/build.sh

# Deploy the application
./scripts/deploy.sh
```

### 3. Access the Application

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
docker compose -f docker-compose.prod.yml --env-file .env.production logs -f

# Specific service
docker compose -f docker-compose.prod.yml --env-file .env.production logs -f server
docker compose -f docker-compose.prod.yml --env-file .env.production logs -f client
docker compose -f docker-compose.prod.yml --env-file .env.production logs -f clickhouse
```

### Check Status
```bash
docker compose -f docker-compose.prod.yml --env-file .env.production ps
```

### Restart Services
```bash
docker compose -f docker-compose.prod.yml --env-file .env.production restart
```

## Environment Configuration

The `.env.production` file controls all deployment settings:

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_DATABASE` | Database name | `biai` |
| `CLICKHOUSE_USER` | Database user | `biai` |
| `CLICKHOUSE_PASSWORD` | Database password | `CHANGE_ME_IN_PRODUCTION` |
| `LOG_LEVEL` | Server log level | `info` |
| `CLIENT_PORT` | Host port for the app | `80` |

**Important:** Change `CLICKHOUSE_PASSWORD` for production deployments!

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
- ✅ Images use pinned versions (node:20-alpine, nginx:1.27-alpine, clickhouse:24.3.4)
- ✅ Non-root users in containers
- ✅ ClickHouse authentication enabled
- Configure TLS/HTTPS (use a reverse proxy like Traefik or Caddy)
- Use Docker secrets for sensitive data in orchestrated environments

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
docker compose -f docker-compose.prod.yml --env-file .env.production logs <service>
```

### Database connection issues
Check if ClickHouse is healthy:
```bash
docker compose -f docker-compose.prod.yml --env-file .env.production exec clickhouse \
  clickhouse-client --user biai --password <your-password> --query "SELECT 1"
```

### Port already in use
```bash
# Find process using port 80
lsof -i :80

# Use a different port by editing .env.production
CLIENT_PORT=8080
```

### Smoke tests fail
The deploy script runs automated smoke tests. If they fail:
```bash
# Check server logs
docker compose -f docker-compose.prod.yml --env-file .env.production logs server

# Check client logs
docker compose -f docker-compose.prod.yml --env-file .env.production logs client

# Test manually
curl http://localhost/health
curl http://localhost/api/datasets
```

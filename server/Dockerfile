# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files (use npm install since no package-lock in workspace)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Build TypeScript
RUN npm run build

# Production stage
FROM node:20-alpine

WORKDIR /app

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Copy package files and install production dependencies only
COPY package*.json ./
RUN npm install --production && npm cache clean --force

# Copy built files from builder stage
COPY --from=builder /app/dist ./dist

# Create required directories with proper permissions
RUN mkdir -p logs uploads && chown -R nodejs:nodejs logs uploads

# Switch to non-root user
USER nodejs

# Expose port
EXPOSE 5001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:5001/health || exit 1

# Start the server
CMD ["node", "dist/index.js"]

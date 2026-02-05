import { Request, Response, NextFunction } from 'express'
import logger from '../utils/logger.js'
import { v4 as uuidv4 } from 'uuid'

// Extend Express Request type to include requestId
declare global {
    namespace Express {
        interface Request {
            requestId?: string
        }
    }
}

// Allowlist of safe query parameter keys to log
// Any keys not in this list will have their values redacted
const SAFE_QUERY_KEYS = new Set([
    'limit', 'offset', 'page', 'sort', 'order', 'format',
    'table', 'column', 'dataset', 'chart', 'type', 'view'
])

function sanitizeQuery(query: Record<string, any>): Record<string, any> {
    const sanitized: Record<string, any> = {}

    for (const [key, value] of Object.entries(query)) {
        if (SAFE_QUERY_KEYS.has(key.toLowerCase())) {
            sanitized[key] = value
        } else {
            sanitized[key] = '[REDACTED]'
        }
    }

    return sanitized
}

export function requestLogger(req: Request, res: Response, next: NextFunction) {
    const requestId = uuidv4()
    const startTime = Date.now()

    // Add request ID to request object for correlation
    req.requestId = requestId

    // Add request ID to response header for client correlation
    res.setHeader('X-Request-Id', requestId)

    // Log incoming request with sanitized query
    logger.info('Incoming request', {
        requestId,
        method: req.method,
        path: req.path,
        query: sanitizeQuery(req.query as Record<string, any>),
        userAgent: req.get('user-agent')
    })

    // Log response when finished
    res.on('finish', () => {
        const duration = Date.now() - startTime
        let logLevel = 'info'

        if (res.statusCode >= 500) {
            logLevel = 'error'
        } else if (res.statusCode >= 400) {
            logLevel = 'warn'
        }

        logger.log(logLevel, 'Request completed', {
            requestId,
            method: req.method,
            path: req.path,
            statusCode: res.statusCode,
            duration: `${duration}ms`
        })
    })

    next()
}

// Export for testing
export { sanitizeQuery, SAFE_QUERY_KEYS }


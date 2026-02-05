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

export function requestLogger(req: Request, res: Response, next: NextFunction) {
    const requestId = uuidv4()
    const startTime = Date.now()

    // Add request ID to request object for correlation
    req.requestId = requestId

    // Redact sensitive query parameters
    const sensitiveKeys = ['token', 'password', 'secret', 'key', 'authorization', 'auth']
    const sanitizedQuery = { ...req.query } as Record<string, any>

    Object.keys(sanitizedQuery).forEach(key => {
        if (sensitiveKeys.some(s => key.toLowerCase().includes(s))) {
            sanitizedQuery[key] = '[REDACTED]'
        }
    })

    // Log incoming request
    logger.info('Incoming request', {
        requestId,
        method: req.method,
        path: req.path,
        query: sanitizedQuery,
        ip: req.ip,
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

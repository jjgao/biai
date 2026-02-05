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

    // Log incoming request
    logger.info('Incoming request', {
        requestId,
        method: req.method,
        path: req.path,
        query: req.query,
        ip: req.ip,
        userAgent: req.get('user-agent')
    })

    // Log response when finished
    res.on('finish', () => {
        const duration = Date.now() - startTime
        const logLevel = res.statusCode >= 400 ? 'warn' : 'info'

        logger[logLevel]('Request completed', {
            requestId,
            method: req.method,
            path: req.path,
            statusCode: res.statusCode,
            duration: `${duration}ms`
        })
    })

    next()
}

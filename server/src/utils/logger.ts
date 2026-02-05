import winston from 'winston'
import fs from 'fs'

// Build transports array
const transports: winston.transport[] = [
    new winston.transports.Console({
        format: winston.format.combine(
            winston.format.colorize(),
            winston.format.printf(({ level, message, timestamp, ...meta }) => {
                const metaStr = Object.keys(meta).length ? JSON.stringify(meta) : ''
                return `${timestamp} ${level}: ${message} ${metaStr}`
            })
        )
    })
]

// Try to add file transports (may fail on read-only filesystems)
const logDir = 'logs'
try {
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir, { recursive: true })
    }
    transports.push(
        new winston.transports.File({
            filename: 'logs/error.log',
            level: 'error'
        }),
        new winston.transports.File({
            filename: 'logs/combined.log'
        })
    )
} catch (err) {
    // File logging disabled - running in read-only environment
    console.warn('File logging disabled: unable to create logs directory')
}

const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
    ),
    transports
})

export default logger


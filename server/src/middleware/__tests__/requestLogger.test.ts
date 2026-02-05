import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { Request, Response, NextFunction } from 'express'
import { requestLogger, sanitizeQuery, SAFE_QUERY_KEYS } from '../requestLogger.js'

// Use vi.hoisted to create mock logger before module is loaded
const mockLogger = vi.hoisted(() => ({
    info: vi.fn(),
    log: vi.fn(),
    warn: vi.fn(),
    error: vi.fn()
}))

// Mock the logger
vi.mock('../../utils/logger.js', () => ({
    default: mockLogger
}))

// Mock uuid
vi.mock('uuid', () => ({
    v4: () => 'test-request-id-123'
}))

describe('sanitizeQuery', () => {
    it('should pass through safe query keys', () => {
        const query = {
            limit: '10',
            offset: '0',
            page: '1',
            sort: 'name',
            order: 'asc'
        }

        const result = sanitizeQuery(query)

        expect(result).toEqual(query)
    })

    it('should redact unknown query keys', () => {
        const query = {
            limit: '10',
            patient_id: 'P12345',
            mrn: '98765',
            subject: 'John Doe'
        }

        const result = sanitizeQuery(query)

        expect(result.limit).toBe('10')
        expect(result.patient_id).toBe('[REDACTED]')
        expect(result.mrn).toBe('[REDACTED]')
        expect(result.subject).toBe('[REDACTED]')
    })

    it('should handle empty query', () => {
        const result = sanitizeQuery({})
        expect(Object.keys(result).length).toBe(0)
    })

    it('should be case-insensitive for safe keys', () => {
        const query = {
            LIMIT: '10',
            Offset: '5',
            PAGE: '2'
        }

        const result = sanitizeQuery(query)

        expect(result.LIMIT).toBe('10')
        expect(result.Offset).toBe('5')
        expect(result.PAGE).toBe('2')
    })

    it('should skip prototype pollution keys', () => {
        const query = {
            limit: '10',
            __proto__: 'malicious',
            constructor: 'attack',
            prototype: 'exploit'
        }

        const result = sanitizeQuery(query)

        expect(result.limit).toBe('10')
        expect('__proto__' in result).toBe(false)
        expect('constructor' in result).toBe(false)
        expect('prototype' in result).toBe(false)
    })

    it('should mark non-string values as [NON-STRING]', () => {
        const query = {
            limit: ['10', '20'],
            offset: { nested: 'object' },
            page: '1'
        }

        const result = sanitizeQuery(query)

        expect(result.limit).toBe('[NON-STRING]')
        expect(result.offset).toBe('[NON-STRING]')
        expect(result.page).toBe('1')
    })
})

describe('requestLogger middleware', () => {
    let mockReq: Partial<Request>
    let mockRes: Partial<Response>
    let mockNext: NextFunction
    let finishHandler: (() => void) | null = null

    beforeEach(() => {
        mockReq = {
            method: 'GET',
            path: '/api/datasets',
            query: { limit: '10' },
            get: vi.fn().mockReturnValue('Mozilla/5.0')
        }

        mockRes = {
            statusCode: 200,
            setHeader: vi.fn(),
            on: vi.fn((event: string, handler: () => void) => {
                if (event === 'finish') {
                    finishHandler = handler
                }
                return mockRes as Response
            })
        }

        mockNext = vi.fn()
    })

    afterEach(() => {
        finishHandler = null
        vi.clearAllMocks()
    })

    it('should attach requestId to request object', () => {
        requestLogger(mockReq as Request, mockRes as Response, mockNext)

        expect(mockReq.requestId).toBe('test-request-id-123')
    })

    it('should set X-Request-Id header on response', () => {
        requestLogger(mockReq as Request, mockRes as Response, mockNext)

        expect(mockRes.setHeader).toHaveBeenCalledWith('X-Request-Id', 'test-request-id-123')
    })

    it('should call next()', () => {
        requestLogger(mockReq as Request, mockRes as Response, mockNext)

        expect(mockNext).toHaveBeenCalled()
    })

    it('should register finish handler', () => {
        requestLogger(mockReq as Request, mockRes as Response, mockNext)

        expect(mockRes.on).toHaveBeenCalledWith('finish', expect.any(Function))
        expect(finishHandler).not.toBeNull()
    })

    it('should log at info level for 2xx status codes', () => {
        mockRes.statusCode = 200
        requestLogger(mockReq as Request, mockRes as Response, mockNext)

        finishHandler!()

        expect(mockLogger.log).toHaveBeenCalledWith('info', 'Request completed', expect.any(Object))
    })

    it('should log at warn level for 4xx status codes', () => {
        mockRes.statusCode = 404
        requestLogger(mockReq as Request, mockRes as Response, mockNext)

        finishHandler!()

        expect(mockLogger.log).toHaveBeenCalledWith('warn', 'Request completed', expect.any(Object))
    })

    it('should log at error level for 5xx status codes', () => {
        mockRes.statusCode = 500
        requestLogger(mockReq as Request, mockRes as Response, mockNext)

        finishHandler!()

        expect(mockLogger.log).toHaveBeenCalledWith('error', 'Request completed', expect.any(Object))
    })
})

describe('SAFE_QUERY_KEYS', () => {
    it('should contain expected safe keys', () => {
        const expectedKeys = ['limit', 'offset', 'page', 'sort', 'order', 'format', 'table', 'column', 'dataset', 'chart', 'type', 'view']

        for (const key of expectedKeys) {
            expect(SAFE_QUERY_KEYS.has(key)).toBe(true)
        }
    })
})


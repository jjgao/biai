import { describe, expect, test, vi } from 'vitest'
import { encodeState, decodeState } from '../urlHelpers'

describe('urlHelpers', () => {
    describe('encodeState', () => {
        test('encodes object to base64 string', () => {
            const state = { foo: 'bar', baz: 123 }
            const encoded = encodeState(state)
            // Expect specific base64 output or just that it decodes back
            expect(typeof encoded).toBe('string')
            expect(encoded.length).toBeGreaterThan(0)
        })

        test('handles special characters via encodeURIComponent', () => {
            const state = { text: 'Hello World & More' }
            const encoded = encodeState(state)
            expect(decodeState(encoded)).toEqual(state)
        })

        test('returns empty string on serialization failure (e.g. circular reference)', () => {
            const circular: any = { self: null }
            circular.self = circular

            // console.error is expected
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => { })

            expect(encodeState(circular)).toBe('')

            consoleSpy.mockRestore()
        })
    })

    describe('decodeState', () => {
        test('decodes base64 string back to object', () => {
            const original = { filters: [{ col: 'a', val: 1 }] }
            const encoded = encodeState(original)
            const decoded = decodeState(encoded)
            expect(decoded).toEqual(original)
        })

        test('returns null for invalid base64', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => { })

            expect(decodeState('invalid-base64')).toBeNull()

            consoleSpy.mockRestore()
        })

        test('returns null for valid base64 but invalid URI component', () => {
            // Just garbage that isn't proper encoded URI
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => { })
            // btoa('test') -> 'dGVzdA=='
            expect(decodeState('dGVzdA==')).toBeNull() // 'test' is not valid JSON
            consoleSpy.mockRestore()
        })
    })
})

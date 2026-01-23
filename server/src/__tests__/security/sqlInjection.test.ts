import { describe, test, expect, vi, beforeEach } from 'vitest'
import {
  validateIdentifier,
  validateIdentifierFormat,
  escapeIdentifier,
  ensurePositiveInteger,
  escapeStringValue
} from '../../utils/sqlSanitizer'

/**
 * SQL Injection Security Tests
 *
 * These tests verify that the SQL sanitizer functions properly block
 * common SQL injection attack vectors used against ClickHouse.
 */
describe('SQL Injection Prevention', () => {
  describe('Classic SQL Injection Attacks', () => {
    const validColumns = new Set(['id', 'name', 'email', 'status'])

    const classicAttacks = [
      "'; DROP TABLE users; --",
      "' OR '1'='1",
      "' OR 1=1 --",
      "' UNION SELECT * FROM users --",
      "1; DELETE FROM users",
      "admin'--",
      "' OR 1=1 #",
      "') OR ('1'='1",
      "1' ORDER BY 1--",
      "1' ORDER BY 2--",
      "' AND '1'='1",
      "' AND SLEEP(5)--",
      "'-'",
      "' '",
      "'&'",
      "'^'",
      "'*'",
    ]

    test.each(classicAttacks)('blocks classic attack: %s', (attack) => {
      expect(() => validateIdentifier(attack, validColumns)).toThrow()
    })
  })

  describe('ClickHouse-Specific Attacks', () => {
    const validColumns = new Set(['id', 'name'])

    const clickhouseAttacks = [
      "id`; DROP TABLE users",         // Backtick injection
      "id); SELECT * FROM system.tables",
      "id SETTINGS log_queries=1",     // Settings injection
      "id FORMAT JSONEachRow",         // Format injection
      "id INTO OUTFILE '/tmp/out'",    // File write attempt
      "id FROM file('/etc/passwd')",   // File read attempt
    ]

    test.each(clickhouseAttacks)('blocks ClickHouse attack: %s', (attack) => {
      expect(() => validateIdentifier(attack, validColumns)).toThrow()
    })
  })

  describe('Comment-Based Injection', () => {
    const validColumns = new Set(['id'])

    const commentAttacks = [
      "id/*comment*/",
      "id--comment",
      "id#comment",
      "id/* */OR/* */1=1",
      "id/**/UNION/**/SELECT/**/password",
    ]

    test.each(commentAttacks)('blocks comment attack: %s', (attack) => {
      expect(() => validateIdentifier(attack, validColumns)).toThrow()
    })
  })

  describe('Whitespace and Encoding Attacks', () => {
    const validColumns = new Set(['id'])

    const encodingAttacks = [
      "id\nUNION SELECT",              // Newline injection
      "id\rDELETE FROM",               // Carriage return
      "id\tOR 1=1",                    // Tab injection
      "id%27",                         // URL encoded quote
      "id%00",                         // Null byte
      "id\x00",                        // Null char
    ]

    test.each(encodingAttacks)('blocks encoding attack: %s', (attack) => {
      expect(() => validateIdentifier(attack, validColumns)).toThrow()
    })
  })

  describe('LIMIT/OFFSET Injection', () => {
    const limitAttacks = [
      '1; DROP TABLE users',
      '1 OR 1=1',
      '1--',
      '1/*comment*/',
      '-1',
      'SLEEP(5)',
      '1e10',
      '0x10',
      'null',
      'undefined',
      'NaN',
      'Infinity',
      '1.5',
      '1,000',
      '1_000',
    ]

    test.each(limitAttacks)('blocks LIMIT injection: %s', (attack) => {
      expect(() => ensurePositiveInteger(attack, 'limit')).toThrow()
    })

    test('accepts valid integers', () => {
      expect(ensurePositiveInteger(0, 'limit')).toBe(0)
      expect(ensurePositiveInteger(1, 'limit')).toBe(1)
      expect(ensurePositiveInteger(100, 'limit')).toBe(100)
      expect(ensurePositiveInteger('50', 'offset')).toBe(50)
    })
  })

  describe('Identifier Escaping', () => {
    test('properly escapes valid identifiers with backticks', () => {
      expect(escapeIdentifier('column_name')).toBe('`column_name`')
      expect(escapeIdentifier('_private')).toBe('`_private`')
      expect(escapeIdentifier('Table123')).toBe('`Table123`')
    })

    test('rejects identifiers with special characters', () => {
      expect(() => escapeIdentifier("col'name")).toThrow()
      expect(() => escapeIdentifier('col"name')).toThrow()
      expect(() => escapeIdentifier('col`name')).toThrow()
      expect(() => escapeIdentifier('col;name')).toThrow()
      expect(() => escapeIdentifier('col--name')).toThrow()
    })

    test('rejects identifiers starting with numbers', () => {
      expect(() => escapeIdentifier('123col')).toThrow()
      expect(() => escapeIdentifier('1table')).toThrow()
    })
  })

  describe('String Value Escaping', () => {
    test('escapes single quotes', () => {
      expect(escapeStringValue("O'Brien")).toBe("O\\'Brien")
      expect(escapeStringValue("it's")).toBe("it\\'s")
      expect(escapeStringValue("''")).toBe("\\'\\'")
    })

    test('escapes backslashes', () => {
      expect(escapeStringValue('path\\to\\file')).toBe('path\\\\to\\\\file')
    })

    test('handles combined escapes', () => {
      expect(escapeStringValue("path\\to\\'file")).toBe("path\\\\to\\\\\\'file")
    })

    test('blocks non-string values', () => {
      expect(() => escapeStringValue(123 as any)).toThrow()
      expect(() => escapeStringValue(null as any)).toThrow()
      expect(() => escapeStringValue(undefined as any)).toThrow()
    })
  })

  describe('Identifier Format Validation', () => {
    test('accepts valid formats', () => {
      expect(validateIdentifierFormat('column_name')).toBe('column_name')
      expect(validateIdentifierFormat('_private')).toBe('_private')
      expect(validateIdentifierFormat('Column123')).toBe('Column123')
      expect(validateIdentifierFormat('a')).toBe('a')
      expect(validateIdentifierFormat('A')).toBe('A')
    })

    test('rejects invalid formats', () => {
      expect(() => validateIdentifierFormat('123start')).toThrow()
      expect(() => validateIdentifierFormat('has-dash')).toThrow()
      expect(() => validateIdentifierFormat('has space')).toThrow()
      expect(() => validateIdentifierFormat('has.dot')).toThrow()
      expect(() => validateIdentifierFormat("has'quote")).toThrow()
      expect(() => validateIdentifierFormat('')).toThrow()
    })
  })

  describe('Whitelist Validation', () => {
    const allowedColumns = new Set(['id', 'name', 'email', 'status', 'created_at'])

    test('accepts values in whitelist', () => {
      expect(validateIdentifier('id', allowedColumns)).toBe('id')
      expect(validateIdentifier('name', allowedColumns)).toBe('name')
      expect(validateIdentifier('created_at', allowedColumns)).toBe('created_at')
    })

    test('rejects values not in whitelist', () => {
      expect(() => validateIdentifier('password', allowedColumns)).toThrow()
      expect(() => validateIdentifier('unknown', allowedColumns)).toThrow()
      expect(() => validateIdentifier('ID', allowedColumns)).toThrow() // case sensitive
    })

    test('trims whitespace before validation', () => {
      expect(validateIdentifier('  id  ', allowedColumns)).toBe('id')
      expect(validateIdentifier('\tname\t', allowedColumns)).toBe('name')
    })

    test('rejects empty values', () => {
      expect(() => validateIdentifier('', allowedColumns)).toThrow()
      expect(() => validateIdentifier('   ', allowedColumns)).toThrow()
    })
  })

  describe('Edge Cases', () => {
    const validColumns = new Set(['column'])

    test('handles very long strings', () => {
      const longAttack = 'a'.repeat(1000) + "'; DROP TABLE"
      expect(() => validateIdentifier(longAttack, validColumns)).toThrow()
    })

    test('handles unicode characters', () => {
      const unicodeAttacks = [
        'column\u0000',          // Null character
        'column\u0027',          // Unicode single quote
        'column\uFF07',          // Fullwidth apostrophe
        'column\u2019',          // Right single quote
      ]

      unicodeAttacks.forEach(attack => {
        expect(() => validateIdentifier(attack, validColumns)).toThrow()
      })
    })

    test('handles array/object inputs gracefully', () => {
      expect(() => validateIdentifier([] as any, validColumns)).toThrow()
      expect(() => validateIdentifier({} as any, validColumns)).toThrow()
      expect(() => validateIdentifier(null as any, validColumns)).toThrow()
      expect(() => validateIdentifier(undefined as any, validColumns)).toThrow()
    })
  })
})

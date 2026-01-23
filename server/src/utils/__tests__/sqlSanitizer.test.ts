import { describe, test, expect } from 'vitest'
import {
  validateIdentifier,
  validateIdentifierFormat,
  escapeIdentifier,
  ensurePositiveInteger,
  sanitizeTableName,
  qualifyTableName,
  escapeStringValue
} from '../sqlSanitizer'

describe('SQL Sanitizer', () => {
  describe('validateIdentifier', () => {
    const validColumns = new Set(['id', 'name', 'status', 'patient_id', 'sample_type'])

    test('accepts valid column names in whitelist', () => {
      expect(validateIdentifier('id', validColumns)).toBe('id')
      expect(validateIdentifier('patient_id', validColumns)).toBe('patient_id')
      expect(validateIdentifier('sample_type', validColumns)).toBe('sample_type')
    })

    test('accepts column names with leading/trailing whitespace', () => {
      expect(validateIdentifier('  id  ', validColumns)).toBe('id')
      expect(validateIdentifier('\tname\t', validColumns)).toBe('name')
    })

    test('rejects column names not in whitelist', () => {
      expect(() => validateIdentifier('unknown_column', validColumns)).toThrow('Invalid column name')
      expect(() => validateIdentifier('password', validColumns)).toThrow('Invalid column name')
    })

    test('rejects SQL injection attempts', () => {
      const attackVectors = [
        "'; DROP TABLE users; --",
        "id; SELECT * FROM passwords",
        "id UNION SELECT password FROM users",
        "1 OR 1=1",
        "id\nUNION SELECT * FROM users",
        "id`; DROP TABLE users; --",
        "id'; DELETE FROM users WHERE '1'='1",
        "id OR 1=1--",
        "id/**/UNION/**/SELECT/**/password/**/FROM/**/users",
      ]

      attackVectors.forEach(attack => {
        expect(() => validateIdentifier(attack, validColumns)).toThrow()
      })
    })

    test('rejects empty and null values', () => {
      expect(() => validateIdentifier('', validColumns)).toThrow('non-empty string')
      expect(() => validateIdentifier('   ', validColumns)).toThrow('non-empty string')
      expect(() => validateIdentifier(null as any, validColumns)).toThrow('non-empty string')
      expect(() => validateIdentifier(undefined as any, validColumns)).toThrow('non-empty string')
    })

    test('rejects excessively long identifiers', () => {
      const longName = 'a'.repeat(200)
      const allowedSet = new Set([longName])
      expect(() => validateIdentifier(longName, allowedSet)).toThrow('exceeds maximum length')
    })
  })

  describe('validateIdentifierFormat', () => {
    test('accepts valid identifier formats', () => {
      expect(validateIdentifierFormat('column_name')).toBe('column_name')
      expect(validateIdentifierFormat('_private')).toBe('_private')
      expect(validateIdentifierFormat('Column123')).toBe('Column123')
      expect(validateIdentifierFormat('a')).toBe('a')
    })

    test('rejects invalid identifier formats', () => {
      expect(() => validateIdentifierFormat('123start')).toThrow('Invalid column name format')
      expect(() => validateIdentifierFormat('has-dash')).toThrow('Invalid column name format')
      expect(() => validateIdentifierFormat('has space')).toThrow('Invalid column name format')
      expect(() => validateIdentifierFormat('has.dot')).toThrow('Invalid column name format')
      expect(() => validateIdentifierFormat("has'quote")).toThrow('Invalid column name format')
    })

    test('rejects SQL injection in format validation', () => {
      expect(() => validateIdentifierFormat("col; DROP TABLE")).toThrow()
      expect(() => validateIdentifierFormat("col'--")).toThrow()
      expect(() => validateIdentifierFormat('col`test')).toThrow()
    })
  })

  describe('escapeIdentifier', () => {
    test('wraps valid identifiers in backticks', () => {
      expect(escapeIdentifier('column_name')).toBe('`column_name`')
      expect(escapeIdentifier('_private')).toBe('`_private`')
      expect(escapeIdentifier('Table123')).toBe('`Table123`')
    })

    test('rejects invalid identifier formats', () => {
      expect(() => escapeIdentifier('has space')).toThrow('Invalid identifier format')
      expect(() => escapeIdentifier('has-dash')).toThrow('Invalid identifier format')
      expect(() => escapeIdentifier("has'quote")).toThrow('Invalid identifier format')
    })

    test('rejects empty values', () => {
      expect(() => escapeIdentifier('')).toThrow('non-empty string')
      expect(() => escapeIdentifier('   ')).toThrow('non-empty string')
    })

    test('rejects SQL injection attempts', () => {
      expect(() => escapeIdentifier("col`; DROP TABLE")).toThrow()
      expect(() => escapeIdentifier("col'; --")).toThrow()
    })
  })

  describe('ensurePositiveInteger', () => {
    test('accepts valid positive integers', () => {
      expect(ensurePositiveInteger(0, 'limit')).toBe(0)
      expect(ensurePositiveInteger(1, 'limit')).toBe(1)
      expect(ensurePositiveInteger(100, 'limit')).toBe(100)
      expect(ensurePositiveInteger(1000000, 'offset')).toBe(1000000)
    })

    test('accepts valid string integers', () => {
      expect(ensurePositiveInteger('0', 'limit')).toBe(0)
      expect(ensurePositiveInteger('50', 'limit')).toBe(50)
      expect(ensurePositiveInteger('  100  ', 'offset')).toBe(100)
    })

    test('rejects negative integers', () => {
      expect(() => ensurePositiveInteger(-1, 'limit')).toThrow('non-negative integer')
      expect(() => ensurePositiveInteger('-5', 'limit')).toThrow('non-negative integer')
    })

    test('rejects non-integer values', () => {
      expect(() => ensurePositiveInteger(1.5, 'limit')).toThrow('non-negative integer')
      expect(() => ensurePositiveInteger('1.5', 'limit')).toThrow('non-negative integer')
      expect(() => ensurePositiveInteger(Infinity, 'limit')).toThrow('non-negative integer')
      expect(() => ensurePositiveInteger(NaN, 'limit')).toThrow('non-negative integer')
    })

    test('rejects SQL injection in LIMIT/OFFSET', () => {
      const attackVectors = [
        '1; DROP TABLE users',
        '1 OR 1=1',
        '1--',
        '1/*comment*/',
        '1e10',
        '0x10',
        'null',
        'undefined',
      ]

      attackVectors.forEach(attack => {
        expect(() => ensurePositiveInteger(attack, 'limit')).toThrow()
      })
    })

    test('rejects non-numeric types', () => {
      expect(() => ensurePositiveInteger(null, 'limit')).toThrow()
      expect(() => ensurePositiveInteger(undefined, 'limit')).toThrow()
      expect(() => ensurePositiveInteger({}, 'limit')).toThrow()
      expect(() => ensurePositiveInteger([], 'limit')).toThrow()
    })
  })

  describe('sanitizeTableName', () => {
    test('converts to lowercase and replaces invalid chars', () => {
      expect(sanitizeTableName('MyTable')).toBe('mytable')
      expect(sanitizeTableName('My Table')).toBe('my_table')
      expect(sanitizeTableName('My-Table!')).toBe('my_table_')
      expect(sanitizeTableName('table@#$name')).toBe('table___name')
    })

    test('handles names starting with numbers', () => {
      expect(sanitizeTableName('123table')).toBe('_123table')
    })

    test('preserves valid names', () => {
      expect(sanitizeTableName('valid_table_name')).toBe('valid_table_name')
      expect(sanitizeTableName('_private_table')).toBe('_private_table')
    })

    test('rejects empty names', () => {
      expect(() => sanitizeTableName('')).toThrow('non-empty string')
      expect(() => sanitizeTableName('   ')).toThrow('non-empty string')
    })

    test('rejects excessively long names', () => {
      const longName = 'a'.repeat(200)
      expect(() => sanitizeTableName(longName)).toThrow('exceeds maximum length')
    })
  })

  describe('qualifyTableName', () => {
    test('creates fully qualified name with escaping', () => {
      expect(qualifyTableName('biai', 'users')).toBe('`biai`.`users`')
      expect(qualifyTableName('my_database', 'my_table')).toBe('`my_database`.`my_table`')
    })

    test('rejects invalid database or table names', () => {
      expect(() => qualifyTableName('invalid db', 'table')).toThrow()
      expect(() => qualifyTableName('db', 'invalid table')).toThrow()
    })
  })

  describe('escapeStringValue', () => {
    test('escapes single quotes', () => {
      expect(escapeStringValue("O'Brien")).toBe("O\\'Brien")
      expect(escapeStringValue("it's")).toBe("it\\'s")
      expect(escapeStringValue("''")).toBe("\\'\\'")
    })

    test('escapes backslashes', () => {
      expect(escapeStringValue('path\\to\\file')).toBe('path\\\\to\\\\file')
      expect(escapeStringValue('C:\\')).toBe('C:\\\\')
    })

    test('escapes both backslashes and quotes', () => {
      expect(escapeStringValue("path\\to\\'file")).toBe("path\\\\to\\\\\\'file")
    })

    test('handles empty strings', () => {
      expect(escapeStringValue('')).toBe('')
    })

    test('handles strings without special characters', () => {
      expect(escapeStringValue('normal string')).toBe('normal string')
      expect(escapeStringValue('12345')).toBe('12345')
    })

    test('rejects non-string values', () => {
      expect(() => escapeStringValue(123 as any)).toThrow('must be a string')
      expect(() => escapeStringValue(null as any)).toThrow('must be a string')
    })
  })

  describe('SQL Injection Attack Vectors', () => {
    const validColumns = new Set(['id', 'name', 'email'])

    test('blocks classic SQL injection patterns', () => {
      const attacks = [
        "' OR '1'='1",
        "'; DROP TABLE users; --",
        "' UNION SELECT * FROM users --",
        "1; DELETE FROM users",
        "admin'--",
        "' OR 1=1 #",
        "') OR ('1'='1",
      ]

      attacks.forEach(attack => {
        expect(() => validateIdentifier(attack, validColumns)).toThrow()
      })
    })

    test('blocks comment-based injection', () => {
      const attacks = [
        'id/*comment*/',
        'id--comment',
        'id#comment',
      ]

      attacks.forEach(attack => {
        expect(() => validateIdentifier(attack, validColumns)).toThrow()
      })
    })

    test('blocks encoding-based attacks', () => {
      const attacks = [
        'id%27',  // URL encoded quote
        'id\x27', // Hex quote
        'id\u0027', // Unicode quote
      ]

      attacks.forEach(attack => {
        expect(() => validateIdentifier(attack, validColumns)).toThrow()
      })
    })

    test('blocks newline/carriage return injection', () => {
      const attacks = [
        "id\n UNION SELECT",
        "id\r\n DROP TABLE",
        "id\rDELETE FROM",
      ]

      attacks.forEach(attack => {
        expect(() => validateIdentifier(attack, validColumns)).toThrow()
      })
    })
  })
})

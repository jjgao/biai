import { describe, test, expect } from 'vitest'
import { parseListValue, looksLikeList, hasNestedLists } from '../listParser'

describe('List Parser', () => {
  describe('parseListValue', () => {
    test('should parse Python-style list with single quotes', () => {
      const result = parseListValue("['item1', 'item2', 'item3']", 'python')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['item1', 'item2', 'item3'])
    })

    test('should parse JSON-style list with double quotes', () => {
      const result = parseListValue('["item1", "item2", "item3"]', 'json')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['item1', 'item2', 'item3'])
    })

    test('should auto-detect Python-style syntax', () => {
      const result = parseListValue("['item1', 'item2']", 'auto')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['item1', 'item2'])
    })

    test('should auto-detect JSON-style syntax', () => {
      const result = parseListValue('["item1", "item2"]', 'auto')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['item1', 'item2'])
    })

    test('should handle list with spaces', () => {
      const result = parseListValue("['Gene expression analysis', 'Copy number analysis']", 'python')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['Gene expression analysis', 'Copy number analysis'])
    })

    test('should handle empty list', () => {
      const result = parseListValue('[]', 'auto')
      expect(result.success).toBe(true)
      expect(result.items).toEqual([])
    })

    test('should handle list with extra whitespace', () => {
      const result = parseListValue("  [ 'item1' , 'item2' ]  ", 'python')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['item1', 'item2'])
    })

    test('should handle single item list', () => {
      const result = parseListValue("['single item']", 'python')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['single item'])
    })

    test('should fail on malformed list', () => {
      const result = parseListValue("['item1', 'item2'", 'python')
      expect(result.success).toBe(false)
      expect(result.error).toBeDefined()
    })

    test('should fail on non-list string', () => {
      const result = parseListValue('just a string', 'auto')
      expect(result.success).toBe(false)
    })

    test('should handle items with commas inside', () => {
      const result = parseListValue('["New York, NY", "Los Angeles, CA"]', 'json')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(['New York, NY', 'Los Angeles, CA'])
    })

    test('should handle items with apostrophes in JSON', () => {
      const result = parseListValue('["It\'s working", "That\'s great"]', 'json')
      expect(result.success).toBe(true)
      expect(result.items).toEqual(["It's working", "That's great"])
    })

    test('should handle real-world analysis types', () => {
      const result = parseListValue(
        "['Gene expression analysis', 'Copy number analysis', 'Survival analysis']",
        'python'
      )
      expect(result.success).toBe(true)
      expect(result.items).toEqual([
        'Gene expression analysis',
        'Copy number analysis',
        'Survival analysis'
      ])
    })
  })

  describe('looksLikeList', () => {
    test('should return true for Python-style list', () => {
      expect(looksLikeList("['item1', 'item2']")).toBe(true)
    })

    test('should return true for JSON-style list', () => {
      expect(looksLikeList('["item1", "item2"]')).toBe(true)
    })

    test('should return true for empty list', () => {
      expect(looksLikeList('[]')).toBe(true)
    })

    test('should return true with extra whitespace', () => {
      expect(looksLikeList("  ['item1']  ")).toBe(true)
    })

    test('should return false for non-list string', () => {
      expect(looksLikeList('just a string')).toBe(false)
    })

    test('should return false for incomplete list', () => {
      expect(looksLikeList("['item1'")).toBe(false)
    })

    test('should return false for empty string', () => {
      expect(looksLikeList('')).toBe(false)
    })
  })

  describe('hasNestedLists', () => {
    test('should return false for JSON nested lists (JSON parsing handles them)', () => {
      // JSON.parse would handle nested arrays, so hasNestedLists returns false for invalid JSON
      expect(hasNestedLists('[["nested"]]')).toBe(false)
    })

    test('should detect Python-style nested lists as containing brackets', () => {
      // The parser will parse the outer list, and items will contain brackets
      expect(hasNestedLists("[['item1'], ['item2']]")).toBe(true)
    })

    test('should return false for simple list', () => {
      expect(hasNestedLists("['item1', 'item2']")).toBe(false)
    })

    test('should return false for empty list', () => {
      expect(hasNestedLists('[]')).toBe(false)
    })

    test('should detect brackets inside parsed items', () => {
      // When successfully parsed, check if items contain brackets
      expect(hasNestedLists("['item with [brackets] inside']")).toBe(true)
    })
  })
})

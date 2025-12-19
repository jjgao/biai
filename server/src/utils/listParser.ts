/**
 * Utility functions for parsing list/array values from CSV files.
 * Supports both Python list syntax ['item1', 'item2'] and JSON array syntax ["item1", "item2"].
 */

export interface ParsedListResult {
  items: string[]
  success: boolean
  error?: string
}

/**
 * Parse a single list value string into an array of items.
 * Supports auto-detection of syntax or explicit Python/JSON parsing.
 *
 * @param value - The string value to parse (e.g., "['item1', 'item2']")
 * @param syntax - 'auto' (default), 'python', or 'json'
 * @returns ParsedListResult with items array and success status
 */
export function parseListValue(
  value: string,
  syntax: 'python' | 'json' | 'auto' = 'auto'
): ParsedListResult {
  if (!value || typeof value !== 'string') {
    return { items: [], success: false, error: 'Invalid input: value must be a non-empty string' }
  }

  const trimmed = value.trim()

  // Check if it looks like a list
  if (!trimmed.startsWith('[') || !trimmed.endsWith(']')) {
    return { items: [], success: false, error: 'Value does not appear to be a list (missing brackets)' }
  }

  // Empty list
  if (trimmed === '[]') {
    return { items: [], success: true }
  }

  // Detect syntax if auto
  let detectedSyntax = syntax
  if (syntax === 'auto') {
    detectedSyntax = detectListSyntax(trimmed)
  }

  // Try parsing with detected/specified syntax
  if (detectedSyntax === 'json') {
    return parseJSONList(trimmed)
  } else {
    return parsePythonList(trimmed)
  }
}

/**
 * Detect whether a list string uses Python or JSON syntax.
 *
 * @param value - The trimmed list string
 * @returns 'python' or 'json'
 */
function detectListSyntax(value: string): 'python' | 'json' {
  // Look for the first quote character after the opening bracket
  const afterBracket = value.substring(1)
  const firstQuote = afterBracket.search(/["']/)

  if (firstQuote === -1) {
    // No quotes found, default to Python
    return 'python'
  }

  const quoteChar = afterBracket[firstQuote]

  // Double quotes suggest JSON, single quotes suggest Python
  return quoteChar === '"' ? 'json' : 'python'
}

/**
 * Parse a JSON array string.
 *
 * @param value - JSON array string like ["item1", "item2"]
 * @returns ParsedListResult
 */
function parseJSONList(value: string): ParsedListResult {
  try {
    const parsed = JSON.parse(value)

    if (!Array.isArray(parsed)) {
      return { items: [], success: false, error: 'Parsed value is not an array' }
    }

    // Convert all items to strings
    const items = parsed.map(item => {
      if (item === null || item === undefined) {
        return ''
      }
      return String(item)
    })

    return { items, success: true }
  } catch (error) {
    return {
      items: [],
      success: false,
      error: `JSON parsing failed: ${error instanceof Error ? error.message : String(error)}`
    }
  }
}

/**
 * Parse a Python list string.
 *
 * @param value - Python list string like ['item1', 'item2']
 * @returns ParsedListResult
 */
function parsePythonList(value: string): ParsedListResult {
  try {
    // Remove outer brackets
    const inner = value.substring(1, value.length - 1).trim()

    if (!inner) {
      return { items: [], success: true }
    }

    // Split by comma, but respect quotes
    const items: string[] = []
    let current = ''
    let inQuotes = false
    let quoteChar = ''
    let escaped = false

    for (let i = 0; i < inner.length; i++) {
      const char = inner[i]

      if (escaped) {
        current += char
        escaped = false
        continue
      }

      if (char === '\\') {
        escaped = true
        continue
      }

      if ((char === '"' || char === "'") && !inQuotes) {
        inQuotes = true
        quoteChar = char
        continue
      }

      if (char === quoteChar && inQuotes) {
        inQuotes = false
        quoteChar = ''
        continue
      }

      if (char === ',' && !inQuotes) {
        items.push(current.trim())
        current = ''
        continue
      }

      current += char
    }

    // Add the last item
    if (current.trim()) {
      items.push(current.trim())
    }

    // Filter out empty items
    const filteredItems = items.filter(item => item.length > 0)

    return { items: filteredItems, success: true }
  } catch (error) {
    return {
      items: [],
      success: false,
      error: `Python list parsing failed: ${error instanceof Error ? error.message : String(error)}`
    }
  }
}

/**
 * Parse multiple list values from a column.
 *
 * @param values - Array of string values to parse
 * @param syntax - 'auto' (default), 'python', or 'json'
 * @returns Array of parsed item arrays, or null for values that failed to parse
 */
export function parseListColumn(
  values: string[],
  syntax: 'python' | 'json' | 'auto' = 'auto'
): Array<string[] | null> {
  return values.map(value => {
    if (!value || value.trim() === '') {
      return null
    }

    const result = parseListValue(value, syntax)
    return result.success ? result.items : null
  })
}

/**
 * Check if a value looks like a list (has bracket syntax).
 *
 * @param value - String value to check
 * @returns true if it looks like a list
 */
export function looksLikeList(value: string): boolean {
  if (!value || typeof value !== 'string') {
    return false
  }

  const trimmed = value.trim()
  return trimmed.startsWith('[') && trimmed.endsWith(']')
}

/**
 * Detect if a value contains a nested list (not supported in v1).
 *
 * @param value - String value to check
 * @returns true if nested lists are detected
 */
export function hasNestedLists(value: string): boolean {
  try {
    const result = parseListValue(value, 'auto')
    if (!result.success) {
      return false
    }

    // Check if any item contains brackets
    return result.items.some(item => item.includes('[') || item.includes(']'))
  } catch {
    return false
  }
}

/**
 * SQL Sanitization Utilities for ClickHouse
 *
 * ClickHouse supports parameterized queries for VALUES only ({param:Type} syntax).
 * Identifiers (column/table names) cannot be parameterized and must be validated
 * against a whitelist and escaped.
 *
 * Security strategy:
 * 1. VALUES: Use ClickHouse parameterized queries
 * 2. IDENTIFIERS: Whitelist validation + backtick escaping as defense-in-depth
 */

// Valid identifier pattern: starts with letter/underscore, followed by alphanumeric/underscore
const IDENTIFIER_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/

// Maximum identifier length (ClickHouse default)
const MAX_IDENTIFIER_LENGTH = 128

/**
 * Validate an identifier against a whitelist of allowed names.
 * Use this for column names and table names that come from user input.
 *
 * @param name - The identifier to validate
 * @param allowedSet - Set of allowed identifier names (from schema)
 * @param entityType - Type of entity for error messages
 * @returns The validated identifier (trimmed)
 * @throws Error if the identifier is not in the whitelist
 *
 * @example
 * const validColumns = new Set(['id', 'name', 'status'])
 * const column = validateIdentifier(userInput, validColumns, 'column')
 */
export function validateIdentifier(
  name: string,
  allowedSet: Set<string>,
  entityType: 'column' | 'table' | 'database' = 'column'
): string {
  if (!name || typeof name !== 'string') {
    throw new Error(`Invalid ${entityType} name: must be a non-empty string`)
  }

  const trimmed = name.trim()

  if (trimmed.length === 0) {
    throw new Error(`Invalid ${entityType} name: must be a non-empty string`)
  }

  if (trimmed.length > MAX_IDENTIFIER_LENGTH) {
    throw new Error(`Invalid ${entityType} name: exceeds maximum length of ${MAX_IDENTIFIER_LENGTH}`)
  }

  if (!allowedSet.has(trimmed)) {
    throw new Error(`Invalid ${entityType} name: '${trimmed}' is not a valid ${entityType}`)
  }

  return trimmed
}

/**
 * Validate that an identifier matches the expected format pattern.
 * Use this for identifiers that come from trusted sources (like database schema)
 * but need format verification as defense-in-depth.
 *
 * @param name - The identifier to validate
 * @param entityType - Type of entity for error messages
 * @returns The validated identifier
 * @throws Error if the identifier doesn't match the expected format
 */
export function validateIdentifierFormat(
  name: string,
  entityType: 'column' | 'table' | 'database' = 'column'
): string {
  if (!name || typeof name !== 'string') {
    throw new Error(`Invalid ${entityType} name: must be a non-empty string`)
  }

  const trimmed = name.trim()

  if (trimmed.length === 0) {
    throw new Error(`Invalid ${entityType} name: must be a non-empty string`)
  }

  if (trimmed.length > MAX_IDENTIFIER_LENGTH) {
    throw new Error(`Invalid ${entityType} name: exceeds maximum length of ${MAX_IDENTIFIER_LENGTH}`)
  }

  if (!IDENTIFIER_REGEX.test(trimmed)) {
    throw new Error(`Invalid ${entityType} name format: '${trimmed}'`)
  }

  return trimmed
}

/**
 * Escape an identifier for safe use in SQL using ClickHouse backtick quoting.
 * Always use after whitelist validation - this is a secondary defense layer.
 *
 * @param name - The identifier to escape (must already be validated)
 * @returns The escaped identifier wrapped in backticks
 * @throws Error if the identifier format is invalid
 *
 * @example
 * const escaped = escapeIdentifier('column_name') // Returns: `column_name`
 */
export function escapeIdentifier(name: string): string {
  if (!name || typeof name !== 'string') {
    throw new Error('Invalid identifier: must be a non-empty string')
  }

  const trimmed = name.trim()

  if (trimmed.length === 0) {
    throw new Error('Invalid identifier: must be a non-empty string')
  }

  // Validate format before escaping (defense in depth)
  if (!IDENTIFIER_REGEX.test(trimmed)) {
    throw new Error(`Invalid identifier format: '${trimmed}'`)
  }

  // ClickHouse uses backticks for identifier quoting
  // Escape any backticks within the name (should not exist if format validated)
  return `\`${trimmed.replace(/`/g, '``')}\``
}

/**
 * Validate and return a non-negative integer for use in LIMIT/OFFSET clauses.
 *
 * @param value - The value to validate (number or string)
 * @param paramName - Name of the parameter for error messages
 * @returns The validated non-negative integer
 * @throws Error if the value is not a valid non-negative integer
 *
 * @example
 * const limit = ensurePositiveInteger(req.query.limit, 'limit')
 */
export function ensurePositiveInteger(value: unknown, paramName: string): number {
  if (typeof value === 'number') {
    if (!Number.isInteger(value) || value < 0 || !Number.isFinite(value)) {
      throw new Error(`Invalid ${paramName}: must be a non-negative integer`)
    }
    return value
  }

  if (typeof value === 'string') {
    const trimmed = value.trim()
    // Strict parsing: only accept digits
    if (!/^\d+$/.test(trimmed)) {
      throw new Error(`Invalid ${paramName}: must be a non-negative integer`)
    }
    const parsed = parseInt(trimmed, 10)
    if (isNaN(parsed) || parsed < 0) {
      throw new Error(`Invalid ${paramName}: must be a non-negative integer`)
    }
    return parsed
  }

  throw new Error(`Invalid ${paramName}: must be a non-negative integer`)
}

/**
 * Sanitize a table name to follow ClickHouse naming conventions.
 * Converts to lowercase and replaces invalid characters with underscores.
 *
 * @param name - The table name to sanitize
 * @returns The sanitized table name
 * @throws Error if the name cannot be sanitized to a valid identifier
 *
 * @example
 * const tableName = sanitizeTableName('My Table!') // Returns: 'my_table_'
 */
export function sanitizeTableName(name: string): string {
  if (!name || typeof name !== 'string') {
    throw new Error('Invalid table name: must be a non-empty string')
  }

  const trimmed = name.trim()

  if (trimmed.length === 0) {
    throw new Error('Invalid table name: must be a non-empty string')
  }

  // Convert to lowercase and replace invalid characters
  let sanitized = trimmed.toLowerCase().replace(/[^a-z0-9_]/g, '_')

  // Ensure it starts with a letter or underscore
  if (/^[0-9]/.test(sanitized)) {
    sanitized = '_' + sanitized
  }

  // Final validation
  if (!IDENTIFIER_REGEX.test(sanitized)) {
    throw new Error(`Invalid table name: '${name}' cannot be sanitized to a valid identifier`)
  }

  if (sanitized.length > MAX_IDENTIFIER_LENGTH) {
    throw new Error(`Invalid table name: exceeds maximum length of ${MAX_IDENTIFIER_LENGTH}`)
  }

  return sanitized
}

/**
 * Build a fully qualified table name from database and table parts.
 * Both parts are escaped for safe SQL use.
 *
 * @param database - The database name (must be validated)
 * @param table - The table name (must be validated)
 * @returns The fully qualified table name: `database`.`table`
 *
 * @example
 * const fqn = qualifyTableName('biai', 'users') // Returns: `biai`.`users`
 */
export function qualifyTableName(database: string, table: string): string {
  return `${escapeIdentifier(database)}.${escapeIdentifier(table)}`
}

/**
 * Escape a string value for safe use in SQL.
 * Use ClickHouse parameterized queries ({param:String}) when possible instead.
 * This is a fallback for complex dynamic SQL where parameterization is not feasible.
 *
 * @param value - The string value to escape
 * @returns The escaped string (without surrounding quotes)
 *
 * @example
 * const escaped = escapeStringValue("O'Brien") // Returns: O\'Brien
 */
export function escapeStringValue(value: string): string {
  if (typeof value !== 'string') {
    throw new Error('Value must be a string')
  }

  // ClickHouse string escaping: escape backslashes first, then single quotes
  return value
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
}

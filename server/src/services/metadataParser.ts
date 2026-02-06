/**
 * Generic metadata parser for dataset and table configuration files
 *
 * Supports two formats:
 * 1. Key-value pairs: key: value
 * 2. Multi-line values with YAML-style lists
 *
 * All fields are preserved, including custom domain-specific fields
 */

export interface TableMetadata {
  // Core fields (used by system)
  data_file?: string
  display_name?: string
  skip_rows?: number
  delimiter?: string
  primary_key?: string
  relationships?: TableRelationship[]

  // Store all other fields as custom metadata
  [key: string]: any
}

export interface TableRelationship {
  foreign_key: string
  references: string  // Format: "table_name(column_name)"
  type?: 'one-to-one' | 'one-to-many' | 'many-to-one' | 'many-to-many'
}

export interface DatasetMetadata {
  // Core fields
  name?: string
  description?: string
  tags?: string[]
  source?: string
  citation?: string
  references?: string[]

  // Store all other fields as custom metadata
  [key: string]: any
}

/**
 * Parse a metadata file in key-value format
 *
 * Format:
 *   key: value
 *   multi_line_key:
 *     - item1
 *     - item2
 *   nested_key: value with : colons
 */
export function parseMetadataFile(content: string): Record<string, any> {
  const lines = content.split('\n')
  const metadata: Record<string, any> = {}
  let currentKey: string | null = null
  let currentArray: string[] = []
  let currentObject: Record<string, any> = {}
  let inArray = false
  let inObject = false
  let baseIndent = 0

  for (let line of lines) {
    // Skip empty lines and comments
    if (!line.trim() || line.trim().startsWith('#')) continue

    // Measure indentation
    const indent = line.length - line.trimLeft().length
    const trimmedLine = line.trim()

    // Check if this is an array item
    if (trimmedLine.startsWith('-')) {
      if (!inArray && currentKey) {
        // First array item — switch from object to array mode
        inArray = true
        inObject = false
        currentArray = []
        baseIndent = indent
      }
      if (inArray && currentKey) {
        currentArray.push(trimmedLine.substring(1).trim())
      }
      continue
    }

    // Parse key-value pair
    const colonIndex = trimmedLine.indexOf(':')
    if (colonIndex === -1) continue

    const key = trimmedLine.substring(0, colonIndex).trim()
    const value = trimmedLine.substring(colonIndex + 1).trim()

    // If we're in an object and see non-indented key, save object
    if (inObject && indent <= baseIndent && currentKey) {
      metadata[currentKey] = currentObject
      currentObject = {}
      inObject = false
      currentKey = null
    }

    // If we're in an array and see non-indented key, save array
    if (inArray && indent <= baseIndent && currentKey) {
      metadata[currentKey] = currentArray
      currentArray = []
      inArray = false
      currentKey = null
    }

    // Nested key-value (part of object)
    if (inObject && indent > baseIndent && currentKey) {
      currentObject[key] = parseValue(value, key)
      continue
    }

    // Top-level key
    if (!value) {
      // Starting a multi-line structure
      currentKey = key
      baseIndent = indent
      // Peek ahead to determine if it's array or object
      // For now, default to object (will switch to array if we see -)
      inObject = true
      currentObject = {}
    } else {
      // Single-line value
      metadata[key] = parseValue(value, key)
    }
  }

  // Save any remaining array or object
  if (inArray && currentKey) {
    metadata[currentKey] = currentArray
  }
  if (inObject && currentKey) {
    metadata[currentKey] = currentObject
  }

  return metadata
}

/**
 * Parse a value, converting to appropriate type
 */
function parseValue(value: string, key: string = ''): any {
  // Boolean
  if (value.toLowerCase() === 'true') return true
  if (value.toLowerCase() === 'false') return false

  // Number
  if (/^\d+$/.test(value)) return parseInt(value, 10)
  if (/^\d+\.\d+$/.test(value)) return parseFloat(value)

  // Array (comma-separated) - only for known array fields
  if (value.includes(',') && ['tags', 'groups'].includes(key)) {
    return value.split(',').map(v => v.trim())
  }

  // String
  return value
}

/**
 * Parse table metadata with relationship parsing
 */
export function parseTableMetadata(content: string): TableMetadata {
  const raw = parseMetadataFile(content)
  const metadata: TableMetadata = { ...raw }

  // Parse relationships if present
  if (raw.relationships) {
    metadata.relationships = parseRelationships(raw.relationships)
  } else if (raw.relationship && typeof raw.relationship === 'object') {
    // Singular nested object format:
    // relationship:
    //   foreign_key: patient_id
    //   references_table: patients
    //   references_column: patient_id
    //   type: many-to-one
    const rel = raw.relationship
    if (rel.foreign_key && rel.references_table && rel.references_column) {
      metadata.relationships = [{
        foreign_key: rel.foreign_key,
        references: `${rel.references_table}(${rel.references_column})`,
        type: rel.type || 'many-to-one'
      }]
    }
  } else if (raw.foreign_key) {
    // Support single foreign_key field format
    metadata.relationships = [parseRelationshipString(raw.foreign_key, raw.references)]
  }

  // Parse numeric fields
  if (raw.skip_rows) {
    metadata.skip_rows = typeof raw.skip_rows === 'number' ? raw.skip_rows : parseInt(raw.skip_rows, 10)
  }

  // Normalize delimiter
  if (raw.delimiter || raw.Delimiter) {
    const delim = (raw.delimiter || raw.Delimiter).toLowerCase()
    metadata.delimiter = delim === 'tab' ? '\t' : delim === 'comma' ? ',' : delim
  }

  return metadata
}

/**
 * Parse relationships array
 */
function parseRelationships(relationships: any): TableRelationship[] {
  if (typeof relationships === 'string') {
    return [parseRelationshipString('', relationships)]
  }

  if (Array.isArray(relationships)) {
    return relationships.map(rel => {
      if (typeof rel === 'string') {
        return parseRelationshipString('', rel)
      }
      return rel as TableRelationship
    })
  }

  return []
}

/**
 * Parse relationship string like "(patient_id) references data_clinical_patient(sample_id)"
 */
function parseRelationshipString(foreignKey: string, references: string): TableRelationship {
  const fkMatch = foreignKey.match(/\(([^)]+)\)/)
  const refMatch = references.match(/([a-zA-Z0-9_]+)\(([^)]+)\)/)

  return {
    foreign_key: fkMatch ? fkMatch[1] : foreignKey,
    references: refMatch ? `${refMatch[1]}(${refMatch[2]})` : references,
    type: 'many-to-one'
  }
}

/**
 * Parse dataset metadata
 */
export function parseDatasetMetadata(content: string): DatasetMetadata {
  const raw = parseMetadataFile(content)
  const metadata: DatasetMetadata = { ...raw }

  // Ensure arrays
  const rawTags = metadata.tags as unknown
  if (typeof rawTags === 'string') {
    metadata.tags = rawTags.split(',').map((t: string) => t.trim())
  }

  const rawReferences = metadata.references as unknown
  if (typeof rawReferences === 'string') {
    metadata.references = rawReferences.split(',').map((r: string) => r.trim())
  }

  return metadata
}

/**
 * Convert metadata object back to file format
 */
export function serializeMetadata(metadata: Record<string, any>): string {
  const lines: string[] = []

  for (const [key, value] of Object.entries(metadata)) {
    if (value === undefined || value === null) continue

    if (Array.isArray(value)) {
      if (value.length === 0) continue
      lines.push(`${key}:`)
      value.forEach(item => {
        lines.push(`  - ${item}`)
      })
    } else if (typeof value === 'object') {
      lines.push(`${key}:`)
      for (const [subKey, subValue] of Object.entries(value)) {
        lines.push(`  ${subKey}: ${subValue}`)
      }
    } else {
      lines.push(`${key}: ${value}`)
    }
  }

  return lines.join('\n')
}

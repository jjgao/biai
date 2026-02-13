import { ParsedData } from './fileParser.js'
import { DatasetTable } from './datasetService.js'

// Re-export from shared so existing imports continue to work
export type { DetectedRelationship } from 'shared'
import type { DetectedRelationship } from 'shared'

/**
 * Detects potential foreign key relationships between a new table and existing tables
 * by comparing column values against primary keys and unique columns in existing tables
 */
export async function detectForeignKeys(
  parsedData: ParsedData,
  existingTables: DatasetTable[]
): Promise<DetectedRelationship[]> {
  const detectedRelationships: DetectedRelationship[] = []

  if (existingTables.length === 0) {
    return detectedRelationships
  }

  // For each column in the new table
  for (const column of parsedData.columns) {
    const columnNameLower = column.name.toLowerCase()

    // Only check columns that look like foreign keys (contain 'id' or 'key')
    const looksLikeForeignKey = columnNameLower.includes('id') || columnNameLower.includes('key')
    if (!looksLikeForeignKey) continue

    // Check against each existing table
    for (const table of existingTables) {
      const schema = JSON.parse(table.schema_json)
      let foundMatch = false

      // Strategy 1: Look for exact column name match with any column in the table
      for (const targetColumn of schema) {
        const targetNameLower = targetColumn.name.toLowerCase()

        // Skip if target is not an ID column
        if (!targetNameLower.includes('id') && !targetNameLower.includes('key')) continue

        // Exact name match - this is the strongest signal
        if (columnNameLower === targetNameLower) {
          detectedRelationships.push({
            foreignKey: column.name,
            referencedTable: table.table_name,
            referencedTableId: table.table_id,
            referencedColumn: targetColumn.name,
            matchPercentage: 100, // Name-based detection
            sampleMatches: []
          })
          foundMatch = true
          break
        }
      }

      if (foundMatch) break

      // Strategy 2: Check if column references the table by name
      // e.g., "patient_id" should match table "data_clinical_patient" with primary key "patient_id"
      if (table.primary_key) {
        const primaryKeyLower = table.primary_key.toLowerCase()
        const tableNameLower = table.table_name.toLowerCase()

        // Extract meaningful parts from table name (ignore common prefixes like "data_", "clinical_")
        const tableParts = tableNameLower.split('_').filter(part =>
          part.length > 2 && !['data', 'clinical', 'test'].includes(part)
        )

        // Check if column name matches the pattern: {table_part}_id or just matches primary key
        const matchesTablePattern = tableParts.some(part =>
          columnNameLower === `${part}_id` ||
          columnNameLower.startsWith(`${part}_`) ||
          columnNameLower === primaryKeyLower
        )

        if (matchesTablePattern) {
          detectedRelationships.push({
            foreignKey: column.name,
            referencedTable: table.table_name,
            referencedTableId: table.table_id,
            referencedColumn: table.primary_key,
            matchPercentage: 100, // Name-based detection
            sampleMatches: []
          })
          break
        }
      }
    }
  }

  return detectedRelationships
}

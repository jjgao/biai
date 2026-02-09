import type { Dataset, Relationship } from './types'

/**
 * Normalize raw API response into a consistent Dataset shape.
 * Handles inconsistent field names from the server (camelCase vs snake_case).
 */
export const normalizeDataset = (raw: any): Dataset => {
  const rawTables = raw.tables || []
  const tableIdToDisplayName: Record<string, string> = {}
  const tableNameToDisplayName: Record<string, string> = {}

  rawTables.forEach((table: any) => {
    const displayName = table.displayName || table.tableDisplayName || table.name || table.table_name || table.id || table.table_id
    if (table.id) tableIdToDisplayName[table.id] = displayName
    if (table.table_id) tableIdToDisplayName[table.table_id] = displayName
    if (table.name) tableNameToDisplayName[table.name] = displayName
    if (table.table_name) tableNameToDisplayName[table.table_name] = displayName
  })

  return {
    ...raw,
    tables: rawTables.map((table: any) => ({
      ...table,
      relationships: (table.relationships || []).map((rel: any) => {
        const referencedTableKey = rel.referencedTable ?? rel.referenced_table
        const referencedDisplayName =
          rel.referencedTableDisplayName ??
          rel.referenced_table_display_name ??
          tableIdToDisplayName[referencedTableKey] ??
          tableNameToDisplayName[referencedTableKey] ??
          referencedTableKey

        return {
          foreignKey: rel.foreignKey ?? rel.foreign_key,
          referencedTable: referencedTableKey,
          referencedColumn: rel.referencedColumn ?? rel.referenced_column,
          type: rel.type ?? rel.relationship_type,
          referencedTableDisplayName: referencedDisplayName
        }
      })
    }))
  }
}

/**
 * Resolve a table ID or name to its display name within a dataset.
 */
export const resolveTableDisplayName = (dataset: Dataset | null, tableIdOrName: string): string => {
  if (!dataset) return tableIdOrName
  const match = dataset.tables.find(
    (t) => t.id === tableIdOrName || t.name === tableIdOrName
  )
  return match?.displayName || match?.name || tableIdOrName
}

/**
 * Enrich relationships with resolved display names for referenced tables.
 */
export const hydrateRelationships = (
  dataset: Dataset | null,
  relationships: Relationship[] = []
): Relationship[] =>
  relationships.map((rel) => ({
    ...rel,
    referencedTableDisplayName: rel.referencedTableDisplayName || resolveTableDisplayName(dataset, rel.referencedTable)
  }))

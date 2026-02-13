/**
 * Filter helper utilities for cross-table filtering
 */

// Re-export from shared so existing imports continue to work
export type { Filter, TableRelationship } from 'shared'
import type { Filter, TableRelationship } from 'shared'

export const ROW_COUNT_KEY = 'rows'

/**
 * Unwrap a filter potentially wrapped in a NOT condition.
 */
export const unwrapNot = (filter?: Filter | null): Filter | undefined => {
  if (!filter) return undefined
  return filter.not ?? filter
}

export interface Table {
  id: string
  name: string
  displayName: string
  rowCount: number
  relationships?: TableRelationship[]
}

/**
 * Extract the column name from a filter tree
 */
export const getFilterColumn = (filter: Filter): string | undefined => {
  if (filter.column) return filter.column
  if (filter.or && Array.isArray(filter.or) && filter.or.length > 0) {
    const child = filter.or[0] as Filter
    return getFilterColumn(child)
  }
  if (filter.and && Array.isArray(filter.and) && filter.and.length > 0) {
    const child = filter.and[0] as Filter
    return getFilterColumn(child)
  }
  if (filter.not) {
    return getFilterColumn(filter.not)
  }
  return undefined
}

/**
 * Extract the table name from a filter
 */
export const getFilterTableName = (filter: Filter): string | undefined => filter.tableName

/**
 * Check if a filter tree contains a specific column
 */
export const filterContainsColumn = (filter: Filter, column: string): boolean => {
  if (filter.column === column) return true
  if (filter.or && Array.isArray(filter.or)) {
    return filter.or.some(child => filterContainsColumn(child, column))
  }
  if (filter.and && Array.isArray(filter.and)) {
    return filter.and.some(child => filterContainsColumn(child, column))
  }
  if (filter.not) {
    return filterContainsColumn(filter.not, column)
  }
  return false
}

/**
 * Find a relationship path between two tables (supports multi-hop transitive relationships).
 *
 * Uses BFS to find the shortest path between tables through foreign key relationships.
 *
 * @returns Array of table names representing the path, or null if no path exists
 *
 * @example
 * // Direct relationship: ['tableA', 'tableB']
 * // Transitive relationship: ['tableA', 'tableB', 'tableC']
 */
export const findRelationshipPath = (
  fromTableName: string,
  toTableName: string,
  allTables: Table[]
): string[] | null => {
  if (fromTableName === toTableName) return null

  // BFS to find shortest path
  const queue: Array<{ tableName: string; path: string[] }> = [
    { tableName: fromTableName, path: [fromTableName] }
  ]
  const visited = new Set<string>([fromTableName])

  while (queue.length > 0) {
    const { tableName: currentTable, path } = queue.shift()!

    // Get current table metadata
    const currentTableMeta = allTables.find(t => t.name === currentTable)
    if (!currentTableMeta) continue

    // Check forward relationships (current table references other tables)
    for (const rel of currentTableMeta.relationships || []) {
      const nextTable = rel.referenced_table
      if (visited.has(nextTable)) continue

      const newPath = [...path, nextTable]

      if (nextTable === toTableName) {
        return newPath
      }

      visited.add(nextTable)
      queue.push({ tableName: nextTable, path: newPath })
    }

    // Check backward relationships (other tables reference current table)
    for (const otherTableMeta of allTables) {
      if (otherTableMeta.name === currentTable) continue

      for (const rel of otherTableMeta.relationships || []) {
        if (rel.referenced_table !== currentTable) continue

        const nextTable = otherTableMeta.name
        if (visited.has(nextTable)) continue

        const newPath = [...path, nextTable]

        if (nextTable === toTableName) {
          return newPath
        }

        visited.add(nextTable)
        queue.push({ tableName: nextTable, path: newPath })
      }
    }
  }

  return null // No path found
}

/**
 * Check if two tables have a relationship (bidirectional, including transitive)
 */
export const tablesHaveRelationship = (
  table1: Table,
  table2: Table,
  allTables: Table[]
): boolean => {
  const path = findRelationshipPath(table1.name, table2.name, allTables)
  return path !== null
}

/**
 * Get all effective filters (direct + propagated) for each table
 */
export const getAllEffectiveFilters = (
  filters: Filter[],
  tables: Table[]
): Record<string, { direct: Filter[]; propagated: Filter[] }> => {
  const result: Record<string, { direct: Filter[]; propagated: Filter[] }> = {}

  // Initialize all tables
  for (const table of tables) {
    result[table.name] = { direct: [], propagated: [] }
  }

  // Group filters by their tableName property
  for (const filter of filters) {
    const filterTableName = getFilterTableName(filter)
    if (!filterTableName) continue

    // This filter belongs to filterTableName
    // It's "direct" for that table, "propagated" for other tables with relationships
    for (const table of tables) {
      if (table.name === filterTableName) {
        // Direct filter
        result[table.name].direct.push(filter)
      } else {
        // Check if there's a relationship between these tables
        const filterTable = tables.find(t => t.name === filterTableName)
        if (!filterTable) continue

        const path = findRelationshipPath(table.name, filterTableName, tables)
        const hasRelationship = path !== null

        if (hasRelationship) {
          // This is a propagated filter for this table
          result[table.name].propagated.push(filter)
        }
      }
    }
  }

  return result
}

/**
 * Generate a unique key for a range filter on a column with a specific count context.
 */
export const rangeKey = (tableName: string, columnName: string, countKey?: string) =>
  `${tableName}.${columnName}:${countKey ?? 'rows'}`

/**
 * Compare two numeric ranges for equality.
 */
export const rangesEqual = (a: { start: number; end: number }, b: { start: number; end: number }) =>
  Math.abs(a.start - b.start) < 1e-9 && Math.abs(a.end - b.end) < 1e-9

/**
 * Get the effective count key for a filter (handling NOT wrappers).
 */
export const getFilterCountKey = (filter: Filter): string => {
  const actual = unwrapNot(filter)
  return actual?.countByKey ?? filter.countByKey ?? ROW_COUNT_KEY
}

/**
 * Create a deep clone of a filter tree.
 * Ensures legacy filters have a countByKey (defaulting to rows).
 */
export const cloneFilterNode = (filter: Filter): Filter => {
  const cloned: Filter = {
    ...filter,
    and: filter.and ? filter.and.map(cloneFilterNode) : undefined,
    or: filter.or ? filter.or.map(cloneFilterNode) : undefined,
    not: filter.not ? cloneFilterNode(filter.not) : undefined,
  }

  // Ensure legacy filters have a countByKey (defaulting to rows)
  if (cloned.countByKey === undefined && cloned.tableName) {
    cloned.countByKey = ROW_COUNT_KEY
  }

  return cloned
}

/**
 * Migrate filters to the current schema.
 * Mainly ensures all filters have a countByKey property.
 */
export const migrateFiltersToCurrentSchema = (filters: Filter[]): Filter[] =>
  filters.map(cloneFilterNode)

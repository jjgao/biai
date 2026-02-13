/**
 * Spreadsheet/import types shared between client and server.
 */

export interface DetectedRelationship {
  foreignKey: string
  referencedTable: string
  referencedColumn: string
  /** @deprecated Use referencedColumn instead. Present in some client-side data. */
  referenced_column?: string
  referencedTableId?: string
  matchPercentage?: number
  sampleMatches?: any[]
}

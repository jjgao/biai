export interface Column {
  name: string
  type: string
  nullable: boolean
}

export interface Relationship {
  foreignKey: string
  referencedTable: string
  referencedColumn: string
  type?: string
  referencedTableDisplayName?: string
}

export interface Table {
  id: string
  name: string
  displayName: string
  filename: string
  rowCount: number
  columns: Column[]
  primaryKey?: string
  relationships?: Relationship[]
  customMetadata?: string
  createdAt: string
}

export interface Dataset {
  id: string
  name: string
  description: string
  tags?: string[]
  source?: string
  citation?: string
  references?: string[]
  customMetadata?: string
  tables: Table[]
  createdAt: string
  updatedAt: string
}

export interface ColumnMetadata {
  column_name: string
  display_name: string
  description: string
  is_hidden: boolean
  display_type: string
  suggested_chart: string
}

export interface ColumnMetadataUpdate {
  displayName?: string
  description?: string
  isHidden?: boolean
  displayType?: string
}

export interface SheetInfo {
  name: string
  rowCount: number
  preview?: any[][]
  columns?: string[]
}

export interface SpreadsheetPreview {
  filename: string
  sheets: SheetInfo[]
}

export interface SheetImportConfig {
  sheetName: string
  tableName: string
  displayName: string
  selected: boolean
  skipRows: number
  primaryKey: string
  relationships: Relationship[]
  importMode: 'append' | 'replace' | 'upsert'
  targetTableId: string
}

export interface PreviewData {
  totalRows: number
  columns: Array<{ name: string; type: string; nullable?: boolean }>
  sampleRows: any[][]
  detectedRelationships?: DetectedRelationship[]
  detectedDelimiter?: string
  detectedSkipRows?: number
  listSuggestions?: ListSuggestion[]
}

export interface DetectedRelationship {
  foreignKey: string
  referencedTable: string
  referencedColumn: string
  referenced_column?: string
  referencedTableId?: string
  matchPercentage?: number
  sampleMatches?: any[]
}

export interface ListSuggestion {
  columnName: string
  listSyntax: 'python' | 'json'
  confidence: 'high' | 'medium' | 'low'
  avgItemCount: number
  uniqueItemCount: number
}

export interface RelationshipFormState {
  foreignKey: string
  referencedTableId: string
  referencedColumn: string
}

export interface PotentialTarget {
  id: string
  name: string
  displayName: string
  columns: string[]
}

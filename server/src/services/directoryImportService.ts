import { readdir, readFile, stat } from 'fs/promises'
import path from 'path'
import { parseDatasetMetadata, parseTableMetadata, TableRelationship } from './metadataParser.js'
import { parseCSVFile, ColumnMetadataConfig } from './fileParser.js'
import datasetService, { TableRelationship as DatasetTableRelationship } from './datasetService.js'

export interface DirectoryImportResult {
  datasetId: string
  datasetName: string
  tables: { tableId: string; tableName: string; displayName: string; rowCount: number }[]
}

interface TableImportEntry {
  metaFile: string
  tableName: string
  displayName: string
  dataFile: string
  dataFilePath: string
  skipRows: number
  delimiter: string
  primaryKey?: string
  relationships: TableRelationship[]
  columnMetadataConfig?: ColumnMetadataConfig
  customMetadata: Record<string, any>
}

const CORE_TABLE_FIELDS = new Set([
  'data_file', 'data_filename', 'table_name', 'display_name',
  'skip_rows', 'delimiter', 'Delimiter', 'primary_key',
  'foreign_key', 'references', 'relationship', 'relationships',
  'column_display_name_row', 'column_description_row',
  'column_datatype_row', 'column_priority_row'
])

const CORE_DATASET_FIELDS = new Set([
  'name', 'description', 'tags', 'source', 'citation', 'references'
])

/**
 * Convert metadataParser's TableRelationship format to datasetService's format.
 *
 * metadataParser: { foreign_key: "patient_id", references: "patients(patient_id)", type: "many-to-one" }
 * datasetService: { foreign_key: "patient_id", referenced_table: "patients", referenced_column: "patient_id", type: "many-to-one" }
 */
function convertRelationship(rel: TableRelationship): DatasetTableRelationship {
  const match = rel.references.match(/^([a-zA-Z0-9_]+)\(([^)]+)\)$/)
  return {
    foreign_key: rel.foreign_key,
    referenced_table: match ? match[1] : rel.references,
    referenced_column: match ? match[2] : '',
    type: rel.type || 'many-to-one'
  }
}

/**
 * Extract custom metadata fields (not in core schema)
 */
function extractCustomMetadata(metadata: Record<string, any>, coreFields: Set<string>): Record<string, any> {
  const custom: Record<string, any> = {}
  for (const [key, value] of Object.entries(metadata)) {
    if (!coreFields.has(key)) {
      custom[key] = value
    }
  }
  return custom
}

/**
 * Topological sort of tables based on relationship dependencies.
 * Tables that are referenced by other tables are imported first.
 */
function topologicalSort(tables: TableImportEntry[]): TableImportEntry[] {
  const tableByName = new Map<string, TableImportEntry>()
  for (const table of tables) {
    tableByName.set(table.tableName, table)
  }

  // Build adjacency list: table -> tables it depends on
  const dependencies = new Map<string, Set<string>>()
  for (const table of tables) {
    dependencies.set(table.tableName, new Set())
  }

  for (const table of tables) {
    if (!table.relationships) continue
    for (const rel of table.relationships) {
      const match = rel.references.match(/^([a-zA-Z0-9_]+)\(/)
      const referencedTable = match ? match[1] : null
      if (referencedTable && tableByName.has(referencedTable)) {
        dependencies.get(table.tableName)!.add(referencedTable)
      }
    }
  }

  // Kahn's algorithm
  const inDegree = new Map<string, number>()
  for (const table of tables) {
    inDegree.set(table.tableName, 0)
  }
  for (const [, deps] of dependencies) {
    for (const dep of deps) {
      inDegree.set(dep, (inDegree.get(dep) || 0) + 1)
    }
  }

  // Wait — inDegree should count incoming edges (tables that depend on this table).
  // Actually for topological sort: if A depends on B, then B must come first.
  // So we need: for each table, how many tables it depends on (outgoing deps).
  // Tables with 0 dependencies go first.

  // Recompute: inDegree = number of dependencies (tables this table depends on)
  const depCount = new Map<string, number>()
  for (const [name, deps] of dependencies) {
    depCount.set(name, deps.size)
  }

  // Reverse adjacency: for each table, which tables depend on it
  const dependents = new Map<string, Set<string>>()
  for (const table of tables) {
    dependents.set(table.tableName, new Set())
  }
  for (const [name, deps] of dependencies) {
    for (const dep of deps) {
      dependents.get(dep)?.add(name)
    }
  }

  const queue: string[] = []
  for (const [name, count] of depCount) {
    if (count === 0) queue.push(name)
  }

  const sorted: TableImportEntry[] = []
  while (queue.length > 0) {
    const name = queue.shift()!
    sorted.push(tableByName.get(name)!)

    for (const dependent of dependents.get(name) || []) {
      const newCount = depCount.get(dependent)! - 1
      depCount.set(dependent, newCount)
      if (newCount === 0) {
        queue.push(dependent)
      }
    }
  }

  // If some tables were not included (circular deps), append them at the end
  if (sorted.length < tables.length) {
    const sortedNames = new Set(sorted.map(t => t.tableName))
    for (const table of tables) {
      if (!sortedNames.has(table.tableName)) {
        sorted.push(table)
      }
    }
  }

  return sorted
}

/**
 * Import a dataset from a local directory containing .meta files.
 *
 * The directory should contain:
 * - dataset.meta (optional): dataset-level metadata (name, description, tags, etc.)
 * - *.meta: one per table, referencing a data file
 * - data files (CSV/TSV/TXT) referenced by the .meta files
 */
export async function importFromDirectory(dirPath: string): Promise<DirectoryImportResult> {
  // Resolve path: if it points to a .meta file, use parent directory
  let resolvedDir = dirPath
  const pathStat = await stat(dirPath)
  if (pathStat.isFile()) {
    resolvedDir = path.dirname(dirPath)
  } else if (!pathStat.isDirectory()) {
    throw new Error(`Path is neither a file nor a directory: ${dirPath}`)
  }

  // Read dataset metadata
  const files = await readdir(resolvedDir)
  let datasetName = path.basename(resolvedDir)
  let datasetDescription = ''
  let datasetTags: string[] = []
  let datasetSource = ''
  let datasetCitation = ''
  let datasetReferences: string[] = []
  let datasetCustomMetadata: Record<string, any> = {}

  const datasetMetaFile = files.find(f => f === 'dataset.meta')
  if (datasetMetaFile) {
    const content = await readFile(path.join(resolvedDir, datasetMetaFile), 'utf8')
    const datasetMeta = parseDatasetMetadata(content)
    datasetName = datasetMeta.name || datasetName
    datasetDescription = datasetMeta.description || ''
    datasetTags = Array.isArray(datasetMeta.tags) ? datasetMeta.tags : (datasetMeta.tags ? [String(datasetMeta.tags)] : [])
    datasetSource = datasetMeta.source || ''
    datasetCitation = datasetMeta.citation || ''
    datasetReferences = Array.isArray(datasetMeta.references) ? datasetMeta.references : (datasetMeta.references ? [String(datasetMeta.references)] : [])
    datasetCustomMetadata = extractCustomMetadata(datasetMeta, CORE_DATASET_FIELDS)
  }

  // Create dataset
  const dataset = await datasetService.createDataset(
    datasetName,
    datasetDescription,
    'system',
    datasetTags,
    datasetSource,
    datasetCitation,
    datasetReferences,
    datasetCustomMetadata
  )

  // Discover and parse table meta files
  const tableMetaFiles = files.filter(f => f.endsWith('.meta') && f !== 'dataset.meta')

  if (tableMetaFiles.length === 0) {
    return {
      datasetId: dataset.dataset_id,
      datasetName: dataset.dataset_name,
      tables: []
    }
  }

  const tableEntries: TableImportEntry[] = []

  for (const metaFile of tableMetaFiles) {
    const content = await readFile(path.join(resolvedDir, metaFile), 'utf8')
    const tableMeta = parseTableMetadata(content)

    const dataFile = tableMeta.data_file || (tableMeta as any).data_filename
    if (!dataFile) {
      console.warn(`No data_file specified in ${metaFile}, skipping`)
      continue
    }

    const dataFilePath = path.join(resolvedDir, dataFile)
    try {
      await stat(dataFilePath)
    } catch {
      console.warn(`Data file ${dataFile} not found for ${metaFile}, skipping`)
      continue
    }

    // Normalize delimiter
    let delimiter = tableMeta.delimiter || '\t'
    if (typeof delimiter === 'string') {
      const lower = delimiter.toLowerCase()
      if (lower === 'tab') delimiter = '\t'
      else if (lower === 'comma') delimiter = ','
    }

    // Build column metadata config
    const columnMetadataConfig: ColumnMetadataConfig = {}
    const raw = tableMeta as any
    if (raw.column_display_name_row !== undefined) {
      columnMetadataConfig.displayNameRow = raw.column_display_name_row
    }
    if (raw.column_description_row !== undefined) {
      columnMetadataConfig.descriptionRow = raw.column_description_row
    }
    if (raw.column_datatype_row !== undefined) {
      columnMetadataConfig.dataTypeRow = raw.column_datatype_row
    }
    if (raw.column_priority_row !== undefined) {
      columnMetadataConfig.priorityRow = raw.column_priority_row
    }

    const tableName = tableMeta.table_name || path.basename(dataFile, path.extname(dataFile))
    if (typeof tableName !== 'string') {
      console.warn(`Invalid table_name in ${metaFile}, skipping`)
      continue
    }

    tableEntries.push({
      metaFile,
      tableName,
      displayName: (tableMeta.display_name as string) || tableName,
      dataFile,
      dataFilePath,
      skipRows: tableMeta.skip_rows || 0,
      delimiter,
      primaryKey: tableMeta.primary_key,
      relationships: tableMeta.relationships || [],
      columnMetadataConfig: Object.keys(columnMetadataConfig).length > 0 ? columnMetadataConfig : undefined,
      customMetadata: extractCustomMetadata(tableMeta, CORE_TABLE_FIELDS)
    })
  }

  // Topological sort for dependency ordering
  const sortedEntries = topologicalSort(tableEntries)

  // Import each table
  const importedTables: DirectoryImportResult['tables'] = []

  for (const entry of sortedEntries) {
    const parsedData = await parseCSVFile(
      entry.dataFilePath,
      entry.skipRows,
      entry.delimiter,
      entry.columnMetadataConfig
    )

    const relationships = entry.relationships.map(convertRelationship)

    const table = await datasetService.addTableToDataset(
      dataset.dataset_id,
      entry.tableName,
      entry.displayName,
      entry.dataFile,
      'text/plain',
      parsedData,
      entry.primaryKey,
      entry.customMetadata,
      relationships
    )

    importedTables.push({
      tableId: table.table_id,
      tableName: table.table_name,
      displayName: table.display_name,
      rowCount: table.row_count
    })
  }

  return {
    datasetId: dataset.dataset_id,
    datasetName: dataset.dataset_name,
    tables: importedTables
  }
}

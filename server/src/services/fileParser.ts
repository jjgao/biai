import { parse } from 'csv-parse'
import { createReadStream } from 'fs'
import { Readable } from 'stream'
import { parseListValue } from '../utils/listParser.js'

export interface ColumnMetadata {
  name: string
  type: 'String' | 'Int32' | 'Float64' | 'DateTime' | 'Boolean' | 'Array(String)'
  nullable: boolean
  index: number
  displayName?: string
  description?: string
  userDataType?: string
  userPriority?: number
  isListColumn?: boolean
  listSyntax?: 'python' | 'json'
}

export interface ParsedData {
  columns: ColumnMetadata[]
  rows: any[][]
  rowCount: number
}

// Infer ClickHouse type from sample values
function inferType(
  values: string[],
  isListColumn: boolean = false
): 'String' | 'Int32' | 'Float64' | 'DateTime' | 'Boolean' | 'Array(String)' {
  // If it's a list column, return Array type
  if (isListColumn) {
    return 'Array(String)'
  }

  const nonEmptyValues = values.filter(v => v !== '' && v !== null && v !== undefined)

  if (nonEmptyValues.length === 0) return 'String'

  // Check for boolean
  const booleanValues = nonEmptyValues.filter(v =>
    v.toLowerCase() === 'true' || v.toLowerCase() === 'false' ||
    v.toLowerCase() === 'yes' || v.toLowerCase() === 'no'
  )
  if (booleanValues.length === nonEmptyValues.length) return 'String' // Store as string for flexibility

  // Check for integer
  const intValues = nonEmptyValues.filter(v => /^-?\d+$/.test(v))
  if (intValues.length === nonEmptyValues.length) return 'Int32'

  // Check for float
  const floatValues = nonEmptyValues.filter(v => /^-?\d*\.?\d+$/.test(v))
  if (floatValues.length === nonEmptyValues.length) return 'Float64'

  // Check for date/datetime
  const dateValues = nonEmptyValues.filter(v => {
    const d = new Date(v)
    return !isNaN(d.getTime())
  })
  if (dateValues.length === nonEmptyValues.length && nonEmptyValues.length > 0) {
    // Only if most values look like dates
    return 'String' // Keep as string for now, can enhance later
  }

  return 'String'
}

export interface ColumnMetadataConfig {
  displayNameRow?: number
  descriptionRow?: number
  dataTypeRow?: number
  priorityRow?: number
}

const COLUMN_FALLBACK_PREFIX = 'column_'

function generateColumnIdentifier(
  rawName: string | undefined,
  index: number,
  usedNames: Set<string>,
  baseNameCounts: Map<string, number>
): string {
  const fallback = `${COLUMN_FALLBACK_PREFIX}${index + 1}`
  const trimmed = (rawName ?? '').trim()

  let base = trimmed.length > 0 ? trimmed : fallback
  base = base.toLowerCase().replace(/[^a-z0-9_]/g, '_')
  base = base.replace(/_+/g, '_').replace(/^_+|_+$/g, '')
  if (!base) {
    base = fallback
  }

  if (!usedNames.has(base)) {
    usedNames.add(base)
    baseNameCounts.set(base, 1)
    return base
  }

  let counter = (baseNameCounts.get(base) ?? 1) + 1
  let candidate = `${base}_${counter}`
  while (usedNames.has(candidate)) {
    counter += 1
    candidate = `${base}_${counter}`
  }

  baseNameCounts.set(base, counter)
  usedNames.add(candidate)
  return candidate
}

/**
 * Detects how many rows to skip by counting rows that start with #
 */
export async function detectSkipRows(
  filePath: string,
  delimiter: string = '\t'
): Promise<number> {
  return new Promise((resolve, reject) => {
    let skipCount = 0
    let rowIndex = 0
    const maxRowsToCheck = 20 // Only check first 20 rows

    const parser = parse({
      delimiter,
      relax_column_count: true,
      skip_empty_lines: true
    })

    createReadStream(filePath)
      .pipe(parser)
      .on('data', (row: string[]) => {
        rowIndex++

        // Stop after checking enough rows
        if (rowIndex > maxRowsToCheck) {
          parser.end()
          return
        }

        // Check if first cell starts with #
        if (row.length > 0 && row[0] && String(row[0]).trim().startsWith('#')) {
          skipCount++
        } else {
          // Once we hit a row that doesn't start with #, we're done
          parser.end()
        }
      })
      .on('end', () => {
        resolve(skipCount)
      })
      .on('error', (error) => {
        reject(error)
      })
  })
}

export async function parseCSVFile(
  filePath: string,
  skipRows: number = 0,
  delimiter: string = '\t',
  columnMetadataConfig?: ColumnMetadataConfig,
  previewOnly: boolean = false,
  listColumns?: Map<string, 'python' | 'json'>
): Promise<ParsedData> {
  return new Promise((resolve, reject) => {
    const rows: any[][] = []
    const allRows: any[][] = [] // Store all rows including metadata
    let headers: string[] = []
    let rowIndex = 0
    const usedColumnNames = new Set<string>()
    const columnNameCounts = new Map<string, number>()

    const parser = parse({
      delimiter,
      relax_column_count: true,
      skip_empty_lines: true
    })

    const stream = createReadStream(filePath)

    stream
      .pipe(parser)
      .on('data', (row: string[]) => {
        rowIndex++
        allRows.push(row) // Store every row

        // Skip metadata rows
        if (rowIndex <= skipRows) {
          return
        }

        // Next row after skip is the header
        if (rowIndex === skipRows + 1) {
          headers = row.map(h => h.trim())
          return
        }

        rows.push(row)

        // For preview mode, stop after reading enough rows
        if (previewOnly && rows.length >= 100) {
          parser.end()
          stream.destroy()
        }
      })
      .on('end', () => {
        // Infer types from first 100 rows
        const sampleSize = Math.min(100, rows.length)
        const columns: ColumnMetadata[] = headers.map((name, index) => {
          const sampleValues = rows.slice(0, sampleSize).map(row => row[index] || '')
          const hasNulls = sampleValues.some(v => v === '' || v === null || v === undefined || v.toLowerCase() === 'na')

          const columnName = generateColumnIdentifier(name, index, usedColumnNames, columnNameCounts)
          const isListColumn = listColumns?.has(columnName) || false
          const listSyntax = listColumns?.get(columnName)

          const column: ColumnMetadata = {
            name: columnName,
            type: inferType(sampleValues, isListColumn),
            nullable: hasNulls,
            index,
            isListColumn,
            listSyntax
          }

          // Extract column metadata from specified rows
          if (columnMetadataConfig) {
            if (columnMetadataConfig.displayNameRow !== undefined && allRows[columnMetadataConfig.displayNameRow]) {
              const displayName = allRows[columnMetadataConfig.displayNameRow][index]
              column.displayName = displayName?.replace(/^#/, '').trim()
            }
            if (columnMetadataConfig.descriptionRow !== undefined && allRows[columnMetadataConfig.descriptionRow]) {
              const description = allRows[columnMetadataConfig.descriptionRow][index]
              column.description = description?.replace(/^#/, '').trim()
            }
            if (columnMetadataConfig.dataTypeRow !== undefined && allRows[columnMetadataConfig.dataTypeRow]) {
              const dataType = allRows[columnMetadataConfig.dataTypeRow][index]
              column.userDataType = dataType?.replace(/^#/, '').trim()
            }
            if (columnMetadataConfig.priorityRow !== undefined && allRows[columnMetadataConfig.priorityRow]) {
              const priority = allRows[columnMetadataConfig.priorityRow][index]
              const priorityStr = priority?.replace(/^#/, '').trim()
              column.userPriority = priorityStr ? parseInt(priorityStr, 10) : undefined
            }
          }

          return column
        })

        // Parse list columns if specified
        if (listColumns && listColumns.size > 0) {
          const listColumnIndices = new Map<number, 'python' | 'json'>()
          columns.forEach(col => {
            if (col.isListColumn && col.listSyntax) {
              listColumnIndices.set(col.index, col.listSyntax)
            }
          })

          // Parse list values in each row
          if (listColumnIndices.size > 0) {
            rows.forEach(row => {
              listColumnIndices.forEach((syntax, colIndex) => {
                const value = row[colIndex]
                if (value && typeof value === 'string' && value.trim() !== '') {
                  const parseResult = parseListValue(value, syntax)
                  row[colIndex] = parseResult.success ? parseResult.items : null
                } else {
                  row[colIndex] = null
                }
              })
            })
          }
        }

        resolve({
          columns,
          rows,
          rowCount: rows.length
        })
      })
      .on('error', (error) => {
        reject(error)
      })
  })
}

export async function parseCSVBuffer(
  buffer: Buffer,
  skipRows: number = 0,
  delimiter: string = '\t',
  listColumns?: Map<string, 'python' | 'json'>
): Promise<ParsedData> {
  return new Promise((resolve, reject) => {
    const rows: any[][] = []
    let headers: string[] = []
    let rowIndex = 0
    const usedColumnNames = new Set<string>()
    const columnNameCounts = new Map<string, number>()

    const parser = parse({
      delimiter,
      relax_column_count: true,
      skip_empty_lines: true
    })

    const stream = Readable.from(buffer)
    stream
      .pipe(parser)
      .on('data', (row: string[]) => {
        rowIndex++

        if (rowIndex <= skipRows) {
          return
        }

        if (rowIndex === skipRows + 1) {
          headers = row.map(h => h.trim())
          return
        }

        rows.push(row)
      })
      .on('end', () => {
        const sampleSize = Math.min(100, rows.length)
        const columns: ColumnMetadata[] = headers.map((name, index) => {
          const sampleValues = rows.slice(0, sampleSize).map(row => row[index] || '')
          const hasNulls = sampleValues.some(v => v === '' || v === null || v === undefined || v.toLowerCase() === 'na')

          const columnName = generateColumnIdentifier(name, index, usedColumnNames, columnNameCounts)
          const isListColumn = listColumns?.has(columnName) || false
          const listSyntax = listColumns?.get(columnName)

          return {
            name: columnName,
            type: inferType(sampleValues, isListColumn),
            nullable: hasNulls,
            index,
            isListColumn,
            listSyntax
          }
        })

        // Parse list columns if specified
        if (listColumns && listColumns.size > 0) {
          const listColumnIndices = new Map<number, 'python' | 'json'>()
          columns.forEach(col => {
            if (col.isListColumn && col.listSyntax) {
              listColumnIndices.set(col.index, col.listSyntax)
            }
          })

          // Parse list values in each row
          if (listColumnIndices.size > 0) {
            rows.forEach(row => {
              listColumnIndices.forEach((syntax, colIndex) => {
                const value = row[colIndex]
                if (value && typeof value === 'string' && value.trim() !== '') {
                  const parseResult = parseListValue(value, syntax)
                  row[colIndex] = parseResult.success ? parseResult.items : null
                } else {
                  row[colIndex] = null
                }
              })
            })
          }
        }

        resolve({
          columns,
          rows,
          rowCount: rows.length
        })
      })
      .on('error', (error) => {
        reject(error)
      })
  })
}

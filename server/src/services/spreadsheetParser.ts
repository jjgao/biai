import XLSX from 'xlsx'
import { ParsedData, ColumnMetadata } from './fileParser.js'
import { parseListValue } from '../utils/listParser.js'

import { DetectedRelationship } from './foreignKeyDetector.js'

export interface SheetInfo {
  name: string
  rowCount: number
  preview?: any[][]
  columns?: string[]
  columnMetadata?: ColumnMetadata[]
  detectedPrimaryKey?: string
  detectedRelationships?: DetectedRelationship[]
}

export interface SpreadsheetPreview {
  filename: string
  sheets: SheetInfo[]
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

function inferType(
  values: any[],
  isListColumn: boolean = false
): 'String' | 'Int32' | 'Float64' | 'DateTime' | 'Boolean' | 'Array(String)' {
  if (isListColumn) {
    return 'Array(String)'
  }

  const nonEmptyValues = values.filter(v => v !== '' && v !== null && v !== undefined)

  if (nonEmptyValues.length === 0) return 'String'

  // Check for boolean
  const booleanValues = nonEmptyValues.filter(v =>
    String(v).toLowerCase() === 'true' || String(v).toLowerCase() === 'false' ||
    String(v).toLowerCase() === 'yes' || String(v).toLowerCase() === 'no' ||
    v === true || v === false
  )
  if (booleanValues.length === nonEmptyValues.length) return 'String'

  // Check for integer
  const intValues = nonEmptyValues.filter(v => {
    if (typeof v === 'number') return Number.isInteger(v)
    return /^-?\d+$/.test(String(v))
  })
  if (intValues.length === nonEmptyValues.length) return 'Int32'

  // Check for float
  const floatValues = nonEmptyValues.filter(v => {
    if (typeof v === 'number') return true
    return /^-?\d*\.?\d+$/.test(String(v))
  })
  if (floatValues.length === nonEmptyValues.length) return 'Float64'

  return 'String'
}

export async function getSpreadsheetPreview(filePath: string): Promise<SpreadsheetPreview> {
  try {
    const workbook = XLSX.readFile(filePath, { type: 'file', cellDates: true })
    const sheets: SheetInfo[] = []

    for (const sheetName of workbook.SheetNames) {
      const sheet = workbook.Sheets[sheetName]
      // Get dimensions
      const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1')
      const rowCount = range.e.r + 1 // 0-indexed

      // Get header row (first row)
      const headers: string[] = []
      for (let C = range.s.c; C <= range.e.c; ++C) {
        const cellAddress = { c: C, r: range.s.r }
        const cellRef = XLSX.utils.encode_cell(cellAddress)
        const cell = sheet[cellRef]
        headers.push(cell ? String(cell.v) : `column_${C + 1}`)
      }

      // Get preview data (first 100 rows for analysis, same as CSV preview)
      const previewDataFull = XLSX.utils.sheet_to_json(sheet, { 
        header: 1, 
        range: { s: { r: 0, c: 0 }, e: { r: 100, c: 1000 } },
        blankrows: false 
      }) as any[][]

      // First row is header (already extracted above, but this gives us data rows)
      const dataRows = previewDataFull.slice(1)
      const previewRows = dataRows.slice(0, 5) // Send 5 rows to frontend for display

      // Analyze columns
      const usedColumnNames = new Set<string>()
      const columnNameCounts = new Map<string, number>()
      
      const columnMetadata: ColumnMetadata[] = headers.map((name, index) => {
        const columnNameStr = String(name || '')
        const sampleValues = dataRows.map(row => row[index])
        const hasNulls = sampleValues.some(v => v === '' || v === null || v === undefined)

        const columnName = generateColumnIdentifier(columnNameStr, index, usedColumnNames, columnNameCounts)
        // Basic type inference
        const type = inferType(sampleValues, false)

        return {
          name: columnName,
          type,
          nullable: hasNulls,
          index,
          isListColumn: false, // Default to false for preview
          displayName: columnNameStr
        }
      })

      // Detect Primary Key (heuristic)
      // Look for columns ending in _id, id, key, or matching sheet name id
      let detectedPrimaryKey: string | undefined
      
      const candidates = columnMetadata.filter(col => {
        const lower = col.name.toLowerCase()
        return lower === 'id' || lower.endsWith('_id') || lower.endsWith('_key')
      })

      if (candidates.length > 0) {
        // Pick the best candidate
        // 1. Exact match 'id' or 'ID'
        const exactId = candidates.find(c => c.name.toLowerCase() === 'id')
        if (exactId) {
          detectedPrimaryKey = exactId.name
        } else {
          // 2. Match with table name (e.g., patient_id in Patients sheet)
          const sheetNameLower = sheetName.toLowerCase().replace(/s$/, '') // simple singularization
          const match = candidates.find(c => c.name.toLowerCase().includes(sheetNameLower))
          if (match) {
            detectedPrimaryKey = match.name
          } else {
            // 3. First candidate
            detectedPrimaryKey = candidates[0].name
          }
        }
      }

      sheets.push({
        name: sheetName,
        rowCount,
        columns: headers,
        preview: previewDataFull.slice(0, 6), // Header + 5 rows
        columnMetadata,
        detectedPrimaryKey
      })
    }

    return {
      filename: filePath.split('/').pop() || 'spreadsheet',
      sheets
    }
  } catch (error) {
    console.error('Error in getSpreadsheetPreview:', error)
    throw error
  }
}

export async function parseSpreadsheetSheet(
  filePath: string,
  sheetName: string,
  skipRows: number = 0,
  listColumns?: Map<string, 'python' | 'json'>
): Promise<ParsedData> {
  const workbook = XLSX.readFile(filePath, { type: 'file', cellDates: true })
  const sheet = workbook.Sheets[sheetName]

  if (!sheet) {
    throw new Error(`Sheet '${sheetName}' not found`)
  }

  // Convert sheet to JSON array of arrays
  // range: skipRows ensures we start reading from the correct row
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, range: skipRows, blankrows: false }) as any[][]

  if (rows.length === 0) {
    return {
      columns: [],
      rows: [],
      rowCount: 0
    }
  }

  // First row is header
  const headerRow = rows[0]
  const dataRows = rows.slice(1)

  const usedColumnNames = new Set<string>()
  const columnNameCounts = new Map<string, number>()

  // Analyze columns
  const sampleSize = Math.min(100, dataRows.length)
  const columns: ColumnMetadata[] = headerRow.map((name: any, index: number) => {
    const columnNameStr = String(name || '')
    const sampleValues = dataRows.slice(0, sampleSize).map(row => row[index])
    const hasNulls = sampleValues.some(v => v === '' || v === null || v === undefined)

    const columnName = generateColumnIdentifier(columnNameStr, index, usedColumnNames, columnNameCounts)
    const isListColumn = listColumns?.has(columnName) || false
    const listSyntax = listColumns?.get(columnName)

    return {
      name: columnName,
      type: inferType(sampleValues, isListColumn),
      nullable: hasNulls,
      index,
      isListColumn,
      listSyntax,
      displayName: columnNameStr
    }
  })

  // Process data rows
  const processedRows = dataRows.map(row => {
    // Ensure row has same length as headers (fill with nulls if needed)
    const paddedRow = new Array(columns.length).fill(null)
    
    columns.forEach((col, index) => {
      let value = row[index]

      // Handle list columns
      if (col.isListColumn && col.listSyntax && value && typeof value === 'string') {
        const parseResult = parseListValue(value, col.listSyntax)
        paddedRow[index] = parseResult.success ? parseResult.items : []
      } else if (col.isListColumn) {
        // If not string or no syntax, default empty array for list column
        paddedRow[index] = []
      } else {
        // Normal value
        // Handle Excel Dates
        if (value instanceof Date) {
            paddedRow[index] = value.toISOString() // Or format as needed
        } else {
            paddedRow[index] = value
        }
      }
    })
    return paddedRow
  })

  return {
    columns,
    rows: processedRows,
    rowCount: processedRows.length
  }
}

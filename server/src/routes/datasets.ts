import express from 'express'
import multer from 'multer'
import { parseCSVFile, detectSkipRows, detectDelimiter } from '../services/fileParser.js'
import { getSpreadsheetPreview, parseSpreadsheetSheet } from '../services/spreadsheetParser.js'
import datasetService from '../services/datasetService.js'
import aggregationService from '../services/aggregationService.js'
import { parseCountByQuery } from '../utils/countBy.js'
import dashboardService from '../services/dashboardService.js'
import { unlink } from 'fs/promises'
import { fetchFileFromUrl } from '../utils/urlFetcher.js'
import { detectForeignKeys } from '../services/foreignKeyDetector.js'
import { detectListColumns } from '../services/columnAnalyzer.js'
import { v4 as uuidv4 } from 'uuid'
import path from 'path'

const router = express.Router()

const upload = multer({
  dest: 'uploads/',
  limits: {
    fileSize: 500 * 1024 * 1024, // 500MB limit for actual upload
    fieldSize: 10 * 1024 * 1024 // 10MB for URL field
  },
  fileFilter: (_req, file, cb) => {
    const allowedTypes = ['.csv', '.txt', '.tsv', '.xlsx', '.xls', '.ods']
    const ext = file.originalname.toLowerCase().substring(file.originalname.lastIndexOf('.'))
    if (allowedTypes.includes(ext)) {
      cb(null, true)
    } else {
      cb(new Error('Only CSV, TSV, TXT, XLSX, XLS, and ODS files are allowed'))
    }
  }
})

const sanitizeConnectionSettings = (raw?: string) => {
  if (!raw) return null
  try {
    const parsed = JSON.parse(raw)
    if (!parsed || !parsed.host) {
      return null
    }
    const { host, port, protocol, username } = parsed
    return { host, port, protocol, username }
  } catch (error) {
    console.warn('Failed to parse connection settings for response:', error)
    return null
  }
}

// Create a new dataset
router.post('/', async (req, res) => {
  try {
    const { name, description = '', tags = [], source = '', citation = '', references = [], customMetadata = {} } = req.body

    if (!name) {
      return res.status(400).json({ error: 'Dataset name is required' })
    }

    const dataset = await datasetService.createDataset(name, description, 'system', tags, source, citation, references, customMetadata)

    return res.json({
      success: true,
      dataset: {
        id: dataset.dataset_id,
        name: dataset.dataset_name,
        description: dataset.description,
        tags: dataset.tags,
        source: dataset.source,
        citation: dataset.citation,
        references: dataset.references,
        createdAt: dataset.created_at
      }
    })
  } catch (error: any) {
    console.error('Create dataset error:', error)
    return res.status(500).json({ error: 'Failed to create dataset', message: error.message })
  }
})

// Connect to existing database
router.post('/connect', async (req, res) => {
  try {
    const {
      databaseName,
      displayName,
      description = '',
      tags = [],
      customMetadata = {},
      host,
      port,
      protocol,
      secure,
      username,
      password
    } = req.body

    if (!databaseName || !displayName || !host) {
      return res.status(400).json({ error: 'Database name, display name, and host are required' })
    }

    const resolvedProtocol: 'http' | 'https' =
      protocol === 'https' || secure === true ? 'https' : 'http'

    const connectionSettings = {
      host,
      port: port !== undefined && port !== null && port !== '' ? Number(port) : undefined,
      protocol: resolvedProtocol,
      username: username || undefined,
      password: password || undefined
    }

    const dataset = await datasetService.connectDatabase(
      databaseName,
      displayName,
      description,
      'system',
      tags,
      customMetadata,
      connectionSettings
    )

    const connectionInfo = sanitizeConnectionSettings(dataset.connection_settings)

    return res.json({
      success: true,
      dataset: {
        id: dataset.dataset_id,
        name: dataset.dataset_name,
        database_name: dataset.database_name,
        database_type: dataset.database_type,
        description: dataset.description,
        tags: dataset.tags,
        connectionInfo,
        createdAt: dataset.created_at
      }
    })
  } catch (error: any) {
    console.error('Connect database error:', error)
    return res.status(500).json({ error: 'Failed to connect database', message: error.message })
  }
})

// Preview table data before importing
router.post('/:id/tables/preview', upload.single('file'), async (req, res) => {
  let tempFilePath: string | null = null

  try {
    const {
      fileUrl,
      skipRows = '0',
      delimiter = '\t'
    } = req.body

    // Handle either file upload or URL
    let filePath: string
    let filename: string

    if (fileUrl) {
      // Fetch file from URL
      tempFilePath = path.join('uploads', `preview_${uuidv4()}`)
      const fetchedFile = await fetchFileFromUrl(fileUrl, tempFilePath)
      filePath = fetchedFile.path
      filename = fetchedFile.filename
    } else if (req.file) {
      // Use uploaded file
      filePath = req.file.path
      filename = req.file.originalname
      tempFilePath = filePath
    } else {
      return res.status(400).json({ error: 'Either file upload or fileUrl is required' })
    }

    // Auto-detect delimiter if not explicitly provided
    let finalDelimiter = delimiter
    let detectedDelimiter: string | undefined

    if (!req.body.delimiter || delimiter === '\t') {
      // Only auto-detect if delimiter wasn't explicitly set by user
      const detected = await detectDelimiter(filePath)
      if (detected !== '\t') {
        detectedDelimiter = detected
        finalDelimiter = detected
      }
    }

    // Auto-detect skipRows if set to 0 - check if first rows start with #
    let finalSkipRows = parseInt(skipRows, 10)
    let detectedSkipRows: number | undefined

    if (finalSkipRows === 0) {
      const detectedRows = await detectSkipRows(filePath, finalDelimiter)
      if (detectedRows > 0) {
        detectedSkipRows = detectedRows
        finalSkipRows = detectedRows
      }
    }

    // Parse the file in preview mode (only reads first ~100 rows)
    const parsedData = await parseCSVFile(
      filePath,
      finalSkipRows,
      finalDelimiter,
      undefined,
      true // previewOnly mode
    )

    // Get existing tables in the dataset to detect potential foreign keys
    const dataset = await datasetService.getDataset(req.params.id)
    const existingTables = dataset?.tables || []

    // Detect potential foreign keys
    const detectedRelationships = await detectForeignKeys(
      parsedData,
      existingTables
    )

    // Detect potential list columns
    const listSuggestions = detectListColumns(
      parsedData.columns,
      parsedData.rows,
      100
    ).filter(r => r.confidence !== 'low') // Only suggest high/medium confidence

    // Clean up temporary file
    await unlink(tempFilePath)
    tempFilePath = null

    // Return preview data with sample rows
    const sampleRows = parsedData.rows.slice(0, 10)

    return res.json({
      success: true,
      preview: {
        filename,
        columns: parsedData.columns,
        sampleRows,
        totalRows: parsedData.rowCount,
        detectedRelationships,
        detectedSkipRows,
        detectedDelimiter,
        listSuggestions
      }
    })
  } catch (error: any) {
    console.error('Preview error:', error)

    if (tempFilePath) {
      try {
        await unlink(tempFilePath)
      } catch (e) {}
    }

    return res.status(500).json({ error: 'Failed to preview table', message: error.message })
  }
})

// Preview spreadsheet sheets
router.post('/:id/spreadsheets/preview', upload.single('file'), async (req, res) => {
  let tempFilePath: string | null = null

  try {
    const { fileUrl } = req.body

    // Handle either file upload or URL
    let filePath: string
    let filename: string

    if (fileUrl) {
      // Fetch file from URL
      tempFilePath = path.join('uploads', `preview_sheet_${uuidv4()}`)
      const fetchedFile = await fetchFileFromUrl(fileUrl, tempFilePath)
      filePath = fetchedFile.path
      filename = fetchedFile.filename
    } else if (req.file) {
      // Use uploaded file
      filePath = req.file.path
      filename = req.file.originalname
      tempFilePath = filePath
    } else {
      return res.status(400).json({ error: 'Either file upload or fileUrl is required' })
    }

    const preview = await getSpreadsheetPreview(filePath)

    // Detect relationships
    try {
      const dataset = await datasetService.getDataset(req.params.id)
      const existingTables = dataset?.tables || []

      // Create virtual tables for the sheets to allow cross-sheet detection
      const sheetVirtualTables = preview.sheets.map(sheet => ({
        table_id: sheet.name.replace(/[^a-z0-9_]/gi, '_').toLowerCase(),
        table_name: sheet.name.replace(/[^a-z0-9_]/gi, '_').toLowerCase(),
        display_name: sheet.name,
        primary_key: sheet.detectedPrimaryKey,
        schema_json: JSON.stringify(sheet.columnMetadata || []),
        // Mock other required fields
        original_filename: '',
        file_type: '',
        row_count: sheet.rowCount,
        clickhouse_table_name: '',
        created_at: new Date()
      }))

      for (let i = 0; i < preview.sheets.length; i++) {
        const sheet = preview.sheets[i]
        if (!sheet.columnMetadata) continue

        // Potential targets: existing tables + other sheets
        const otherSheets = sheetVirtualTables.filter((_, idx) => idx !== i)
        const potentialTargets = [...existingTables, ...otherSheets] as any[]

        const parsedDataStub = {
          columns: sheet.columnMetadata,
          rows: [],
          rowCount: sheet.rowCount
        }

        sheet.detectedRelationships = await detectForeignKeys(parsedDataStub, potentialTargets)
      }
    } catch (e) {
      console.warn('Failed to detect relationships for spreadsheet:', e)
      // Continue without relationships
    }

    // Clean up temporary file
    await unlink(tempFilePath)
    tempFilePath = null

    return res.json({
      success: true,
      preview
    })
  } catch (error: any) {
    console.error('Spreadsheet preview error:', error)

    if (tempFilePath) {
      try {
        await unlink(tempFilePath)
      } catch (e) {}
    }

    return res.status(500).json({ error: 'Failed to preview spreadsheet', message: error.message })
  }
})

// Import spreadsheet sheets as tables
router.post('/:id/spreadsheets/import', upload.single('file'), async (req, res) => {
  let tempFilePath: string | null = null

  try {
    req.setTimeout(600000) // 10 minutes
    res.setTimeout(600000)

    const {
      fileUrl,
      sheetsConfig: sheetsConfigStr
    } = req.body

    let sheetsConfig: any[]
    try {
      sheetsConfig = JSON.parse(sheetsConfigStr)
      if (!Array.isArray(sheetsConfig) || sheetsConfig.length === 0) {
        throw new Error('Invalid sheets configuration')
      }
    } catch (e) {
      return res.status(400).json({ error: 'Invalid sheets configuration JSON' })
    }

    // Handle either file upload or URL
    let filePath: string
    let filename: string
    let mimetype: string

    if (fileUrl) {
      // Fetch file from URL
      tempFilePath = path.join('uploads', `import_sheet_${uuidv4()}`)
      const fetchedFile = await fetchFileFromUrl(fileUrl, tempFilePath)
      filePath = fetchedFile.path
      filename = fetchedFile.filename
      mimetype = fetchedFile.mimetype
    } else if (req.file) {
      // Use uploaded file
      filePath = req.file.path
      filename = req.file.originalname
      mimetype = req.file.mimetype
      tempFilePath = filePath
    } else {
      return res.status(400).json({ error: 'Either file upload or fileUrl is required' })
    }

    const importedTables = []

    for (const sheetConfig of sheetsConfig) {
      const { sheetName, tableName, displayName, skipRows = 0, primaryKey, relationships = [] } = sheetConfig

      // Parse sheet data
      const parsedData = await parseSpreadsheetSheet(
        filePath,
        sheetName,
        skipRows
      )

      if (parsedData.columns.length === 0) {
        console.warn(`Skipping empty sheet: ${sheetName}`)
        continue
      }

      // Add table to dataset
      const table = await datasetService.addTableToDataset(
        req.params.id,
        tableName,
        displayName || tableName,
        filename, // Using spreadsheet filename for all tables
        mimetype,
        parsedData,
        primaryKey,
        {}, // Custom metadata
        relationships // Pass detected relationships
      )

      importedTables.push({
        id: table.table_id,
        name: table.table_name,
        rowCount: table.row_count
      })
    }

    await unlink(tempFilePath)
    tempFilePath = null

    return res.json({
      success: true,
      importedTables
    })
  } catch (error: any) {
    console.error('Spreadsheet import error:', error)

    if (tempFilePath) {
      try {
        await unlink(tempFilePath)
      } catch (e) {}
    }

    return res.status(500).json({ error: 'Failed to import spreadsheet', message: error.message })
  }
})

// Add table to existing dataset
router.post('/:id/tables', upload.single('file'), async (req, res) => {
  let tempFilePath: string | null = null

  try {
    // Set a longer timeout for large file processing
    req.setTimeout(600000) // 10 minutes
    res.setTimeout(600000)
    const {
      fileUrl,
      tableName,
      displayName,
      skipRows = '0',
      delimiter = '\t',
      primaryKey,
      customMetadata = '{}',
      relationships = '[]',
      columnMetadataConfig = '{}',
      listColumns = '{}'
    } = req.body

    // Handle either file upload or URL
    let filePath: string
    let filename: string
    let mimetype: string

    if (fileUrl) {
      // Fetch file from URL
      tempFilePath = path.join('uploads', `url_${uuidv4()}`)
      const fetchedFile = await fetchFileFromUrl(fileUrl, tempFilePath)
      filePath = fetchedFile.path
      filename = fetchedFile.filename
      mimetype = fetchedFile.mimetype
    } else if (req.file) {
      // Use uploaded file
      filePath = req.file.path
      filename = req.file.originalname
      mimetype = req.file.mimetype
      tempFilePath = filePath
    } else {
      return res.status(400).json({ error: 'Either file upload or fileUrl is required' })
    }

    if (!tableName) {
      await unlink(tempFilePath)
      return res.status(400).json({ error: 'Table name is required' })
    }

    // Validate table name (alphanumeric and underscores only)
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      await unlink(tempFilePath)
      return res.status(400).json({ error: 'Table name must contain only letters, numbers, and underscores' })
    }

    // Parse column metadata config
    let parsedColumnMetadataConfig
    try {
      const config = JSON.parse(columnMetadataConfig)
      if (Object.keys(config).length > 0) {
        parsedColumnMetadataConfig = config
      }
    } catch (e) {
      // Ignore parse errors, just don't use column metadata
    }

    // Parse list columns configuration
    let parsedListColumns: Map<string, 'python' | 'json'> | undefined
    try {
      const listColumnsObj = JSON.parse(listColumns)
      if (Object.keys(listColumnsObj).length > 0) {
        parsedListColumns = new Map(Object.entries(listColumnsObj)) as Map<string, 'python' | 'json'>
      }
    } catch (e) {
      // Ignore parse errors, just don't use list columns
    }

    const parsedData = await parseCSVFile(
      filePath,
      parseInt(skipRows, 10),
      delimiter,
      parsedColumnMetadataConfig,
      false, // not preview mode
      parsedListColumns
    )

    // Parse JSON fields
    let parsedCustomMetadata = {}
    let parsedRelationships = []
    try {
      parsedCustomMetadata = JSON.parse(customMetadata)
      parsedRelationships = JSON.parse(relationships)
    } catch (e) {
      await unlink(tempFilePath)
      return res.status(400).json({ error: 'Invalid JSON in customMetadata or relationships' })
    }

    const table = await datasetService.addTableToDataset(
      req.params.id,
      tableName,
      displayName || tableName,
      filename,
      mimetype,
      parsedData,
      primaryKey,
      parsedCustomMetadata,
      parsedRelationships
    )

    await unlink(tempFilePath)
    tempFilePath = null

    return res.json({
      success: true,
      table: {
        id: table.table_id,
        name: table.table_name,
        displayName: table.display_name,
        filename: table.original_filename,
        rowCount: table.row_count,
        columns: parsedData.columns.length
      }
    })
  } catch (error: any) {
    console.error('Add table error:', error)

    if (tempFilePath) {
      try {
        await unlink(tempFilePath)
      } catch (e) {}
    }

    return res.status(500).json({ error: 'Failed to add table', message: error.message })
  }
})

// List all datasets
router.get('/', async (_req, res) => {
  try {
    const datasets = await datasetService.listDatasets()
    res.json({
      datasets: datasets.map(d => ({
        id: d.dataset_id,
        name: d.dataset_name,
        database_name: d.database_name,
        database_type: d.database_type,
        description: d.description,
        tags: d.tags,
        source: d.source,
        citation: d.citation,
        references: d.references,
        tableCount: d.tables?.length || 0,
        tables: d.tables?.map(t => ({
          id: t.table_id,
          name: t.table_name,
          displayName: t.display_name,
          rowCount: t.row_count
        })),
        connectionInfo: sanitizeConnectionSettings(d.connection_settings),
        createdAt: d.created_at,
        updatedAt: d.updated_at
      }))
    })
  } catch (error: any) {
    console.error('List datasets error:', error)
    res.status(500).json({ error: 'Failed to list datasets', message: error.message })
  }
})

// Get dataset details
router.get('/:id', async (req, res) => {
  try {
    const dataset = await datasetService.getDataset(req.params.id)
    if (!dataset) {
      return res.status(404).json({ error: 'Dataset not found' })
    }

    const connectionInfo = sanitizeConnectionSettings(dataset.connection_settings)

    return res.json({
      dataset: {
        id: dataset.dataset_id,
        name: dataset.dataset_name,
        database_name: dataset.database_name,
        database_type: dataset.database_type,
        description: dataset.description,
        tags: dataset.tags,
        source: dataset.source,
        citation: dataset.citation,
        references: dataset.references,
        customMetadata: dataset.custom_metadata,
        connectionInfo,
        tables: dataset.tables?.map(t => ({
          id: t.table_id,
          name: t.table_name,
          displayName: t.display_name,
          filename: t.original_filename,
          rowCount: t.row_count,
          columns: t.schema_json ? JSON.parse(t.schema_json) : [],
          primaryKey: t.primary_key,
          customMetadata: t.custom_metadata,
          relationships: t.relationships,
          createdAt: t.created_at
        })),
        createdBy: dataset.created_by,
        createdAt: dataset.created_at,
        updatedAt: dataset.updated_at
      }
    })
  } catch (error: any) {
    console.error('Get dataset error:', error)
    return res.status(500).json({ error: 'Failed to get dataset', message: error.message })
  }
})

// Get table data
router.get('/:id/tables/:tableId/data', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 100
    const offset = parseInt(req.query.offset as string) || 0

    const data = await datasetService.getTableData(req.params.id, req.params.tableId, limit, offset)

    res.json({
      data,
      limit,
      offset,
      count: data.length
    })
  } catch (error: any) {
    console.error('Get table data error:', error)
    res.status(500).json({ error: 'Failed to get table data', message: error.message })
  }
})

// Get table column metadata
router.get('/:id/tables/:tableId/columns', async (req, res) => {
  try {
    const columns = await datasetService.getTableColumns(req.params.id, req.params.tableId)

    res.json({
      columns
    })
  } catch (error: any) {
    console.error('Get table columns error:', error)
    res.status(500).json({ error: 'Failed to get table columns', message: error.message })
  }
})

// Update table primary key
router.patch('/:id/tables/:tableId/primary-key', async (req, res) => {
  try {
    const primaryKey = req.body.primaryKey === undefined ? null : req.body.primaryKey
    if (primaryKey !== null && typeof primaryKey !== 'string') {
      return res.status(400).json({ error: 'Primary key must be a string or null' })
    }

    await datasetService.updatePrimaryKey(req.params.id, req.params.tableId, primaryKey)
    res.json({ success: true })
  } catch (error: any) {
    console.error('Update primary key error:', error)
    res.status(500).json({ error: 'Failed to update primary key', message: error.message })
  }
})

// Add table relationship
router.post('/:id/tables/:tableId/relationships', async (req, res) => {
  try {
    const { foreignKey, referencedTableId, referencedColumn, type } = req.body

    if (!foreignKey || !referencedTableId || !referencedColumn) {
      return res.status(400).json({ error: 'foreignKey, referencedTableId, and referencedColumn are required' })
    }

    await datasetService.addRelationship(req.params.id, req.params.tableId, {
      foreign_key: foreignKey,
      referenced_table: referencedTableId,
      referenced_column: referencedColumn,
      type
    })

    res.json({ success: true })
  } catch (error: any) {
    console.error('Add relationship error:', error)
    res.status(500).json({ error: 'Failed to add relationship', message: error.message })
  }
})

// Remove table relationship
router.delete('/:id/tables/:tableId/relationships', async (req, res) => {
  try {
    const { foreignKey, referencedTable, referencedColumn } = req.query

    if (!foreignKey || !referencedTable || !referencedColumn) {
      return res.status(400).json({ error: 'foreignKey, referencedTable, and referencedColumn are required' })
    }

    await datasetService.deleteRelationship(
      req.params.id,
      req.params.tableId,
      {
        foreign_key: String(foreignKey),
        referenced_table: String(referencedTable),
        referenced_column: String(referencedColumn)
      }
    )

    res.json({ success: true })
  } catch (error: any) {
    console.error('Delete relationship error:', error)
    res.status(500).json({ error: 'Failed to delete relationship', message: error.message })
  }
})

// Update column metadata
router.patch('/:id/tables/:tableId/columns/:columnName', async (req, res) => {
  try {
    const { displayName, description, isHidden, displayType } = req.body

    await datasetService.updateColumnMetadata(
      req.params.id,
      req.params.tableId,
      req.params.columnName,
      { displayName, description, isHidden, displayType }
    )

    res.json({ success: true, message: 'Column metadata updated' })
  } catch (error: any) {
    console.error('Update column metadata error:', error)
    res.status(500).json({ error: 'Failed to update column metadata', message: error.message })
  }
})

// Delete dataset
router.delete('/:id', async (req, res) => {
  try {
    await datasetService.deleteDataset(req.params.id)
    res.json({ success: true, message: 'Dataset deleted successfully' })
  } catch (error: any) {
    console.error('Delete dataset error:', error)
    res.status(500).json({ error: 'Failed to delete dataset', message: error.message })
  }
})

// Delete table from dataset
router.delete('/:id/tables/:tableId', async (req, res) => {
  try {
    await datasetService.deleteTable(req.params.id, req.params.tableId)
    res.json({ success: true, message: 'Table deleted successfully' })
  } catch (error: any) {
    console.error('Delete table error:', error)
    res.status(500).json({ error: 'Failed to delete table', message: error.message })
  }
})

// Get aggregated data for all columns in a table
router.get('/:id/tables/:tableId/aggregations', async (req, res) => {
  try {
    // Parse filters from query string
    let filters = []
    if (req.query.filters) {
      try {
        filters = JSON.parse(req.query.filters as string)
      } catch (e) {
        return res.status(400).json({ error: 'Invalid filters JSON' })
      }
    }

    const rawCountBy = typeof req.query.countBy === 'string' ? req.query.countBy : undefined
    const { config: countByConfig, error: countByError } = parseCountByQuery(rawCountBy)
    if (countByError) {
      return res.status(400).json({ error: countByError })
    }

    const aggregations = await aggregationService.getTableAggregations(
      req.params.id,
      req.params.tableId,
      filters,
      countByConfig
    )

    return res.json({
      aggregations
    })
  } catch (error: any) {
    const status = error?.status || 500
    console.error('Get table aggregations error:', error)
    return res.status(status).json({
      error: status === 400 ? 'Invalid countBy parameter' : 'Failed to get table aggregations',
      message: error.message
    })
  }
})

// Get aggregated data for a specific column
router.get('/:id/tables/:tableId/columns/:columnName/aggregation', async (req, res) => {
  try {
    const { displayType } = req.query

    if (!displayType) {
      return res.status(400).json({ error: 'displayType query parameter is required' })
    }

    const rawCountBy = typeof req.query.countBy === 'string' ? req.query.countBy : undefined
    const { config: countByConfig, error: countByError } = parseCountByQuery(rawCountBy)
    if (countByError) {
      return res.status(400).json({ error: countByError })
    }

    const aggregation = await aggregationService.getColumnAggregation(
      req.params.id,
      req.params.tableId,
      req.params.columnName,
      displayType as string,
      [],
      undefined,
      undefined,
      countByConfig
    )

    return res.json({
      aggregation
    })
  } catch (error: any) {
    const status = error?.status || 500
    console.error('Get column aggregation error:', error)
    return res.status(status).json({
      error: status === 400 ? 'Invalid countBy parameter' : 'Failed to get column aggregation',
      message: error.message
    })
  }
})

// Get survival curve for a time/status column pair
router.get('/:id/tables/:tableId/survival', async (req, res) => {
  try {
    const { timeColumn, statusColumn } = req.query
    if (!timeColumn || !statusColumn) {
      return res.status(400).json({ error: 'timeColumn and statusColumn query parameters are required' })
    }

    let filters = []
    if (req.query.filters) {
      try {
        filters = JSON.parse(req.query.filters as string)
      } catch (e) {
        return res.status(400).json({ error: 'Invalid filters JSON' })
      }
    }

    const rawCountBy = typeof req.query.countBy === 'string' ? req.query.countBy : undefined
    const { config: countByConfig, error: countByError } = parseCountByQuery(rawCountBy)
    if (countByError) {
      return res.status(400).json({ error: countByError })
    }

    const curve = await aggregationService.getSurvivalCurve(
      req.params.id,
      req.params.tableId,
      String(timeColumn),
      String(statusColumn),
      filters,
      countByConfig
    )

    return res.json({ curve })
  } catch (error: any) {
    const status = error?.status || 500
    console.error('Get survival curve error:', error)
    return res.status(status).json({
      error: status === 400 ? 'Invalid request' : 'Failed to get survival curve',
      message: error.message
    })
  }
})

// Dashboard routes

// Get all dashboards for a dataset
router.get('/:id/dashboards', async (req, res) => {
  try {
    const dashboards = await dashboardService.listDashboards(req.params.id)
    return res.json({ dashboards })
  } catch (error: any) {
    console.error('List dashboards error:', error)
    return res.status(500).json({ error: 'Failed to list dashboards', message: error.message })
  }
})

// Get a specific dashboard
router.get('/:id/dashboards/:dashboardId', async (req, res) => {
  try {
    const dashboard = await dashboardService.getDashboard(req.params.id, req.params.dashboardId)
    if (!dashboard) {
      return res.status(404).json({ error: 'Dashboard not found' })
    }
    return res.json({ dashboard })
  } catch (error: any) {
    console.error('Get dashboard error:', error)
    return res.status(500).json({ error: 'Failed to get dashboard', message: error.message })
  }
})

// Save/update a dashboard
router.post('/:id/dashboards', async (req, res) => {
  try {
    const { dashboard_id, dashboard_name, charts, is_most_recent = false } = req.body

    if (!dashboard_id || !dashboard_name || !Array.isArray(charts)) {
      return res.status(400).json({ error: 'dashboard_id, dashboard_name, and charts array are required' })
    }

    const dashboard = await dashboardService.saveDashboard(
      req.params.id,
      dashboard_id,
      dashboard_name,
      charts,
      is_most_recent
    )

    return res.json({ dashboard })
  } catch (error: any) {
    console.error('Save dashboard error:', error)
    return res.status(500).json({ error: 'Failed to save dashboard', message: error.message })
  }
})

// Delete a dashboard
router.delete('/:id/dashboards/:dashboardId', async (req, res) => {
  try {
    await dashboardService.deleteDashboard(req.params.id, req.params.dashboardId)
    return res.json({ success: true })
  } catch (error: any) {
    console.error('Delete dashboard error:', error)
    return res.status(500).json({ error: 'Failed to delete dashboard', message: error.message })
  }
})

export default router

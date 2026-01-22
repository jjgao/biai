import clickhouseClient, { createClickHouseClient, ClickHouseConnectionSettings } from '../config/clickhouse.js'
import { v4 as uuidv4 } from 'uuid'
import { ColumnMetadata, ParsedData } from './fileParser.js'
import { analyzeColumn } from './columnAnalyzer.js'
import { escapeIdentifier } from '../utils/sqlSanitizer.js'

export interface TableRelationship {
  foreign_key: string
  referenced_table: string
  referenced_column: string
  type?: string
}

export interface DatasetTable {
  table_id: string
  table_name: string
  display_name: string
  original_filename: string
  file_type: string
  row_count: number
  clickhouse_table_name: string
  schema_json: string
  primary_key?: string
  custom_metadata?: string
  relationships?: TableRelationship[]
  created_at: string | Date
}

export interface Dataset {
  dataset_id: string
  dataset_name: string
  database_name: string
  database_type: 'created' | 'connected'
  description: string
  tags?: string[]
  source?: string
  citation?: string
  references?: string[]
  custom_metadata?: string
  connection_settings?: string
  created_by: string
  created_at: string | Date
  updated_at: string | Date
  tables?: DatasetTable[]
}

export interface DatasetConnectionSettings extends ClickHouseConnectionSettings {
  protocol?: 'http' | 'https'
  port?: number
  username?: string
  password?: string
}

export class DatasetService {
  private parseConnectionSettings(raw?: string): DatasetConnectionSettings | null {
    if (!raw) return null
    try {
      const parsed = JSON.parse(raw) as DatasetConnectionSettings
      if (!parsed || !parsed.host) {
        return null
      }
      return parsed
    } catch (error) {
      console.warn('Failed to parse connection settings:', error)
      return null
    }
  }

  private sanitizeConnectionSettings(settings: DatasetConnectionSettings | null) {
    if (!settings) return null
    const { host, port, protocol, username } = settings
    return { host, port, protocol, username }
  }

  private normalizeDisplayType(displayType?: string): string {
    return displayType || ''
  }

  private getDatasetConnectionSettings(dataset: Dataset): DatasetConnectionSettings | null {
    return this.parseConnectionSettings(dataset.connection_settings)
  }

  private async executeWithClient<T>(
    settings: DatasetConnectionSettings | null,
    database: string,
    fn: (client: any) => Promise<T>
  ): Promise<T> {
    if (!settings) {
      return fn(clickhouseClient)
    }

    const client = createClickHouseClient({
      ...settings,
      database: database || settings.database || 'default'
    })

    try {
      return await fn(client)
    } finally {
      await client.close()
    }
  }

  // Create a new empty dataset with its own ClickHouse database
  async createDataset(
    name: string,
    description: string = '',
    createdBy: string = 'system',
    tags: string[] = [],
    source: string = '',
    citation: string = '',
    references: string[] = [],
    customMetadata: Record<string, any> = {}
  ): Promise<Dataset> {
    const datasetId = uuidv4()
    // Create a unique database name from the dataset name
    const databaseName = `ds_${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}_${datasetId.substring(0, 8)}`
    const databaseType = 'created'

    // Create the ClickHouse database
    // databaseName is already sanitized, escape as defense-in-depth
    await clickhouseClient.command({
      query: `CREATE DATABASE IF NOT EXISTS ${escapeIdentifier(databaseName)}`
    })

    await clickhouseClient.insert({
      table: 'biai.datasets_metadata',
      values: [{
        dataset_id: datasetId,
        dataset_name: name,
        database_name: databaseName,
        database_type: databaseType,
        description: description,
        tags: tags,
        source: source,
        citation: citation,
        references: references,
        custom_metadata: JSON.stringify(customMetadata),
        connection_settings: '',
        created_by: createdBy
      }],
      format: 'JSONEachRow'
    })

    return {
      dataset_id: datasetId,
      dataset_name: name,
      database_name: databaseName,
      database_type: databaseType,
      description,
      tags,
      source,
      citation,
      references,
      custom_metadata: JSON.stringify(customMetadata),
      connection_settings: '',
      created_by: createdBy,
      created_at: new Date(),
      updated_at: new Date(),
      tables: []
    }
  }

  // Register an existing ClickHouse database as a connected dataset
  async connectDatabase(
    databaseName: string,
    displayName: string,
    description: string = '',
    createdBy: string = 'system',
    tags: string[] = [],
    customMetadata: Record<string, any> = {},
    connectionSettings: DatasetConnectionSettings
  ): Promise<Dataset> {
    if (!connectionSettings || !connectionSettings.host) {
      throw new Error('Connection settings with host are required')
    }

    const datasetId = uuidv4()
    const databaseType = 'connected'
    const connectionSettingsJson = JSON.stringify(connectionSettings)

    await clickhouseClient.insert({
      table: 'biai.datasets_metadata',
      values: [{
        dataset_id: datasetId,
        dataset_name: displayName,
        database_name: databaseName,
        database_type: databaseType,
        description: description,
        tags: tags,
        source: '',
        citation: '',
        references: [],
        custom_metadata: JSON.stringify(customMetadata),
        connection_settings: connectionSettingsJson,
        created_by: createdBy
      }],
      format: 'JSONEachRow'
    })

    return {
      dataset_id: datasetId,
      dataset_name: displayName,
      database_name: databaseName,
      database_type: databaseType,
      description,
      tags,
      custom_metadata: JSON.stringify(customMetadata),
      connection_settings: connectionSettingsJson,
      created_by: createdBy,
      created_at: new Date(),
      updated_at: new Date(),
      tables: []
    }
  }

  // Add a table to an existing dataset
  async addTableToDataset(
    datasetId: string,
    tableName: string,
    displayName: string,
    filename: string,
    fileType: string,
    parsedData: ParsedData,
    primaryKey?: string,
    customMetadata: Record<string, any> = {},
    relationships: TableRelationship[] = []
  ): Promise<DatasetTable> {
    // Get the dataset to find its database name
    const dataset = await this.getDataset(datasetId)
    if (!dataset || !dataset.database_name) {
      throw new Error('Dataset not found')
    }

    // In the new simplified model, use the tableName directly (after sanitization)
    const cleanTableName = tableName.replace(/[^a-z0-9_]/g, '_').toLowerCase()
    const fullTableName = `${dataset.database_name}.${cleanTableName}`

    // Create the ClickHouse table in the dataset's database
    await this.createDynamicTable(dataset.database_name, cleanTableName, parsedData.columns, primaryKey)

    // Insert data
    await this.insertData(dataset.database_name, cleanTableName, parsedData.columns, parsedData.rows)

    // Update dataset timestamp
    await clickhouseClient.command({
      query: 'ALTER TABLE biai.datasets_metadata UPDATE updated_at = now() WHERE dataset_id = {datasetId:String}',
      query_params: { datasetId }
    })

    // Get row count from the newly created table
    const countResult = await clickhouseClient.query({
      query: `SELECT count() as cnt FROM ${fullTableName}`,
      format: 'JSONEachRow'
    })
    const countData = await countResult.json<{ cnt: string }>()
    const rowCount = parseInt(countData[0]?.cnt || '0', 10)

    const tableRecord = {
      dataset_id: datasetId,
      table_id: cleanTableName,
      table_name: cleanTableName,
      display_name: displayName,
      original_filename: filename,
      file_type: fileType,
      row_count: rowCount,
      clickhouse_table_name: fullTableName,
      schema_json: JSON.stringify(parsedData.columns),
      primary_key: primaryKey,
      custom_metadata: JSON.stringify(customMetadata),
      created_at: Math.floor(Date.now() / 1000)
    }

    await clickhouseClient.insert({
      table: 'biai.dataset_tables',
      values: [tableRecord],
      format: 'JSONEachRow'
    })

    // Store column metadata with analysis
    const columnValues = []
    for (const col of parsedData.columns) {
      const analysis = await analyzeColumn(
        fullTableName,
        col.name,
        col.type
      )

      const finalPriority = col.userPriority !== undefined ? col.userPriority : analysis.display_priority

      columnValues.push({
        dataset_id: datasetId,
        table_id: cleanTableName,
        column_name: col.name,
        column_type: col.type,
        column_index: col.index,
        is_nullable: col.nullable,
        display_name: col.displayName || '',
        description: col.description || '',
        user_data_type: col.userDataType || '',
        user_priority: col.userPriority !== undefined ? col.userPriority : null,
        display_type: analysis.display_type,
        unique_value_count: analysis.unique_value_count,
        null_count: analysis.null_count,
        min_value: analysis.min_value,
        max_value: analysis.max_value,
        suggested_chart: analysis.suggested_chart,
        display_priority: finalPriority,
        is_hidden: analysis.is_hidden,
        is_list_column: col.isListColumn || false,
        list_syntax: col.listSyntax || ''
      })
    }

    if (columnValues.length > 0) {
      await clickhouseClient.insert({
        table: 'biai.dataset_columns',
        values: columnValues,
        format: 'JSONEachRow'
      })
    }

    // Store relationships
    if (relationships && relationships.length > 0) {
      const relationshipValues = relationships.map(rel => ({
        dataset_id: datasetId,
        table_id: cleanTableName,
        foreign_key: rel.foreign_key,
        referenced_table: rel.referenced_table,
        referenced_column: rel.referenced_column,
        relationship_type: rel.type || 'many-to-one'
      }))

      await clickhouseClient.insert({
        table: 'biai.table_relationships',
        values: relationshipValues,
        format: 'JSONEachRow'
      })
    }

    return {
      table_id: cleanTableName,
      table_name: cleanTableName,
      display_name: displayName,
      original_filename: filename,
      file_type: fileType,
      row_count: rowCount,
      clickhouse_table_name: fullTableName,
      schema_json: JSON.stringify(parsedData.columns),
      primary_key: primaryKey,
      custom_metadata: JSON.stringify(customMetadata),
      relationships,
      created_at: new Date()
    }
  }

  private async createDynamicTable(databaseName: string, tableName: string, columns: ColumnMetadata[], primaryKey?: string): Promise<void> {
    const columnDefs = columns.map(col => {
      let columnType: string

      // Handle list/array columns
      if (col.isListColumn || col.type === 'Array(String)') {
        // Array types cannot be nullable in ClickHouse
        columnType = 'Array(String)'
      } else {
        // Regular columns - make nullable except for primary key
        const shouldBeNullable = col.name !== primaryKey
        columnType = shouldBeNullable ? `Nullable(${col.type})` : col.type
      }

      // Escape column name for SQL safety
      return `${escapeIdentifier(col.name)} ${columnType}`
    }).join(',\n    ')

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${escapeIdentifier(databaseName)}.${escapeIdentifier(tableName)} (
        ${columnDefs}
      ) ENGINE = MergeTree()
      ORDER BY tuple()
    `

    await clickhouseClient.command({ query: createTableQuery })
  }

  private async insertData(
    databaseName: string,
    tableName: string,
    columns: ColumnMetadata[],
    rows: any[][]
  ): Promise<void> {
    if (rows.length === 0) return

    const values = rows.map(row => {
      const obj: any = {}
      columns.forEach((col, index) => {
        let value = row[index]

        // Handle list/array columns first (arrays cannot be null in ClickHouse)
        if (col.isListColumn || col.type === 'Array(String)') {
          // Empty values become empty arrays, not null
          if (value === '' || value === null || value === undefined || value === 'NA') {
            obj[col.name] = []
            return
          }
          // Value should already be parsed as array from parseCSVFile
          obj[col.name] = Array.isArray(value) ? value : []
          return
        }

        // Handle empty values for non-array columns
        if (value === '' || value === null || value === undefined || value === 'NA') {
          obj[col.name] = null
          return
        }

        // Handle other column types
        if (col.type === 'Int32') {
          const parsed = parseInt(value, 10)
          obj[col.name] = isNaN(parsed) ? null : parsed
        } else if (col.type === 'Float64') {
          const parsed = parseFloat(value)
          obj[col.name] = isNaN(parsed) ? null : parsed
        } else {
          obj[col.name] = value
        }
      })
      return obj
    })

    const batchSize = 1000
    for (let i = 0; i < values.length; i += batchSize) {
      const batch = values.slice(i, i + batchSize)
      await clickhouseClient.insert({
        table: `${databaseName}.${tableName}`,
        values: batch,
        format: 'JSONEachRow'
      })
    }
  }

  async listDatasets(): Promise<Dataset[]> {
    const result = await clickhouseClient.query({
      query: 'SELECT * FROM biai.datasets_metadata ORDER BY created_at DESC',
      format: 'JSONEachRow'
    })

    const datasets = await result.json<Dataset>()

    // Load tables for all datasets in parallel (performance optimization)
    await Promise.all(
      datasets.map(async (dataset) => {
        const connectionSettings = this.getDatasetConnectionSettings(dataset)

        if (dataset.database_type === 'connected' && dataset.database_name) {
          if (connectionSettings?.host) {
            try {
              dataset.tables = await this.getDatabaseTables(
                dataset.database_name,
                connectionSettings,
                dataset.dataset_id
              )
            } catch (error) {
              console.warn(`Remote table sync failed for dataset ${dataset.dataset_id}:`, error)
              dataset.tables = await this.getDatasetTables(dataset.dataset_id)
              this.updateCustomMetadata(dataset, { remote_table_sync_failed: true })
            }
          } else {
            dataset.tables = await this.getDatasetTables(dataset.dataset_id)
            this.updateCustomMetadata(dataset, { remote_connection_missing: true })
          }
        } else {
          dataset.tables = await this.getDatasetTables(dataset.dataset_id)
        }
      })
    )

    return datasets
  }

  private async getDatabaseTables(
    databaseName: string,
    connectionSettings?: DatasetConnectionSettings,
    datasetId?: string
  ): Promise<DatasetTable[]> {
    // In the new simplified model, always load tables from the database directly
    // No fallback to biai.dataset_tables

    return this.executeWithClient(connectionSettings ?? null, databaseName, async (client) => {
      const tablesResult = await client.query({
        query: `
          SELECT name, engine, total_rows
          FROM system.tables
          WHERE database = {database:String}
            AND name NOT LIKE '.%'
          ORDER BY name
        `,
        query_params: { database: databaseName },
        format: 'JSONEachRow'
      })

      const tables = await tablesResult.json<{ name: string; engine: string; total_rows: string }>()

      let tablesWithSchema = await Promise.all(
        tables.map(async (table) => {
          const columnsResult = await client.query({
            query: `
              SELECT name, type, position
              FROM system.columns
              WHERE database = {database:String}
                AND table = {table:String}
              ORDER BY position
            `,
            query_params: { database: databaseName, table: table.name },
            format: 'JSONEachRow'
          })

          const columns = await columnsResult.json<{ name: string; type: string; position: number }>()
          const normalizedColumns = columns.map(col => ({
            name: col.name,
            type: col.type.startsWith('Nullable(') && col.type.endsWith(')')
              ? col.type.slice(9, -1)
              : col.type,
            nullable: col.type.includes('Nullable'),
            position: col.position ?? 0
          }))

          const tableMetadata: DatasetTable = {
            table_id: table.name,
            table_name: table.name,
            display_name: table.name,
            original_filename: '',
            file_type: '',
            row_count: parseInt(table.total_rows, 10) || 0,
            clickhouse_table_name: `${databaseName}.${table.name}`,
            schema_json: JSON.stringify(
              normalizedColumns.map(col => ({
                name: col.name,
                type: col.type,
                nullable: col.nullable
              }))
            ),
            primary_key: null,
            custom_metadata: '{}',
            relationships: [],
            created_at: Math.floor(Date.now() / 1000)
          }

          if (datasetId) {
            await this.syncConnectedTableMetadata(datasetId, tableMetadata)
            await this.syncConnectedColumns(
              datasetId,
              table.name,
              normalizedColumns.map(col => ({
                name: col.name,
                type: col.type,
                nullable: col.nullable,
                position: Math.max(col.position - 1, 0)
              }))
            )
          }

          return tableMetadata
        })
      )

      if (datasetId) {
        const storedTables = await this.getDatasetTables(datasetId)
        const storedMap = new Map(storedTables.map(table => [table.table_id, table]))

        tablesWithSchema = tablesWithSchema.map(table => {
          const stored = storedMap.get(table.table_id)
          if (!stored) return table
          return {
            ...table,
            display_name: stored.display_name || table.display_name,
            primary_key: stored.primary_key ?? table.primary_key,
            custom_metadata: stored.custom_metadata ?? table.custom_metadata,
            relationships: stored.relationships && stored.relationships.length > 0 ? stored.relationships : table.relationships
          }
        })
      }

      return tablesWithSchema
    })
  }

  async getDataset(datasetId: string): Promise<Dataset | null> {
    const dataset = await this.getDatasetMetadata(datasetId)
    if (!dataset) return null

    if (!dataset.database_name) {
      dataset.tables = await this.getDatasetTables(datasetId)
      return dataset
    }

    const connectionSettings = this.getDatasetConnectionSettings(dataset)

    if (dataset.database_type === 'connected') {
      if (connectionSettings?.host) {
        try {
          dataset.tables = await this.getDatabaseTables(
            dataset.database_name,
            connectionSettings,
            dataset.dataset_id
          )
        } catch (error) {
          console.warn(`Remote dataset load failed for ${dataset.dataset_id}:`, error)
          dataset.tables = await this.getDatasetTables(datasetId)
          this.updateCustomMetadata(dataset, { remote_table_sync_failed: true })
        }
      } else {
        dataset.tables = await this.getDatasetTables(datasetId)
        this.updateCustomMetadata(dataset, { remote_connection_missing: true })
      }
    } else {
      dataset.tables = await this.getDatasetTables(datasetId)
    }

    return dataset
  }

  async getDatasetMetadata(datasetId: string): Promise<Dataset | null> {
    const result = await clickhouseClient.query({
      query: 'SELECT * FROM biai.datasets_metadata WHERE dataset_id = {datasetId:String}',
      query_params: { datasetId },
      format: 'JSONEachRow'
    })

    const data = await result.json<Dataset>()
    if (data.length === 0) return null

    return data[0]
  }

  async getDatasetTables(datasetId: string): Promise<DatasetTable[]> {
    const result = await clickhouseClient.query({
      query: 'SELECT * FROM biai.dataset_tables WHERE dataset_id = {datasetId:String} ORDER BY created_at',
      query_params: { datasetId },
      format: 'JSONEachRow'
    })

    const tables = result ? await result.json<DatasetTable>() : []

    // Load relationships for each table
    for (const table of tables) {
      const relResult = await clickhouseClient.query({
        query: `
          SELECT *
          FROM biai.table_relationships
          WHERE dataset_id = {datasetId:String}
            AND table_id = {tableId:String}
        `,
        query_params: { datasetId, tableId: table.table_id },
        format: 'JSONEachRow'
      })

      const relationships = relResult ? await relResult.json<{
        foreign_key: string
        referenced_table: string
        referenced_column: string
        relationship_type: string
      }>() : []
      table.relationships = relationships.map(rel => ({
        foreign_key: rel.foreign_key,
        referenced_table: rel.referenced_table,
        referenced_column: rel.referenced_column,
        type: rel.relationship_type
      }))
    }

    return tables
  }

  async getTableData(datasetId: string, tableId: string, limit: number = 100, offset: number = 0): Promise<any[]> {
    const dataset = await this.getDataset(datasetId)
    if (!dataset || !dataset.database_name) {
      throw new Error('Dataset not found')
    }

    const connectionSettings = this.getDatasetConnectionSettings(dataset)

    if (dataset.database_type === 'connected') {
      if (!connectionSettings || !connectionSettings.host) {
        const storedResult = await clickhouseClient.query({
          query: `
            SELECT
              column_name,
              column_type,
              column_index,
              is_nullable,
              display_name,
              description,
              user_data_type,
              user_priority,
              display_type,
              unique_value_count,
              null_count,
              min_value,
              max_value,
              suggested_chart,
              display_priority,
              is_hidden
            FROM biai.dataset_columns
            WHERE dataset_id = {datasetId:String} AND table_id = {tableId:String}
            ORDER BY column_index, created_at DESC
            LIMIT 1 BY column_name
          `,
          query_params: { datasetId, tableId },
          format: 'JSONEachRow'
        })

        return await storedResult.json<Record<string, unknown>>()
      }

      return this.executeWithClient(connectionSettings, dataset.database_name, async (client) => {
        const result = await client.query({
          query: `SELECT * FROM ${dataset.database_name}.${tableId} LIMIT ${limit} OFFSET ${offset}`,
          format: 'JSONEachRow'
        })
        return await result.json<Record<string, unknown>>()
      })
    }

    let qualifiedTableName: string | null = null

    const tableInfo = dataset.tables?.find(t => t.table_id === tableId)
    if (tableInfo && tableInfo.clickhouse_table_name) {
      qualifiedTableName = this.qualifyTableName(tableInfo.clickhouse_table_name)
    }

    if (!qualifiedTableName) {
      const tableResult = await clickhouseClient.query({
        query: `
          SELECT clickhouse_table_name
          FROM biai.dataset_tables
          WHERE dataset_id = {datasetId:String}
            AND table_id = {tableId:String}
          LIMIT 1
        `,
        query_params: { datasetId, tableId },
        format: 'JSONEachRow'
      })

      const tables = await tableResult.json<{ clickhouse_table_name: string }>()
      if (tables.length === 0) {
        throw new Error('Table not found')
      }
      qualifiedTableName = this.qualifyTableName(tables[0].clickhouse_table_name)
    }

    const result = await clickhouseClient.query({
      query: `SELECT * FROM ${qualifiedTableName} LIMIT ${limit} OFFSET ${offset}`,
      format: 'JSONEachRow'
    })

    return await result.json<Record<string, unknown>>()
  }

  async getTableColumns(datasetId: string, tableId: string): Promise<any[]> {
    const dataset = await this.getDataset(datasetId)
    if (!dataset || !dataset.database_name) {
      throw new Error('Dataset not found')
    }

    const connectionSettings = this.getDatasetConnectionSettings(dataset)

    if (dataset.database_type === 'connected' && (!connectionSettings || !connectionSettings.host)) {
      return this.getStoredColumnMetadata(datasetId, tableId)
    }

    // Always use stored column metadata for uploaded datasets
    return this.getStoredColumnMetadata(datasetId, tableId)
  }

  private inferDisplayType(columnType: string, columnName: string): string {
    const nameLower = columnName.toLowerCase()
    if (nameLower.includes('id') || nameLower.includes('key')) return 'id'
    if (columnType.includes('Int') || columnType.includes('Float') || columnType.includes('Decimal')) {
      return 'numeric'
    }
    if (columnType.includes('Date')) return 'datetime'
    return 'categorical'
  }

  private async syncConnectedTableMetadata(datasetId: string, table: DatasetTable): Promise<void> {
    const existingResult = await clickhouseClient.query({
      query: `
        SELECT 1
        FROM biai.dataset_tables
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
        LIMIT 1
      `,
      query_params: { datasetId, tableId: table.table_id },
      format: 'JSONEachRow'
    })

    const exists = (await existingResult.json()).length > 0

    if (!exists) {
      await clickhouseClient.insert({
        table: 'biai.dataset_tables',
        values: [{
          dataset_id: datasetId,
          table_id: table.table_id,
          table_name: table.table_name,
          display_name: table.display_name,
          original_filename: table.original_filename,
          file_type: table.file_type,
          row_count: table.row_count,
          clickhouse_table_name: table.clickhouse_table_name,
          schema_json: table.schema_json,
          primary_key: null,
          custom_metadata: '{}',
          created_at: Math.floor(Date.now() / 1000)
        }],
        format: 'JSONEachRow'
      })
      return
    }

    await clickhouseClient.command({
      query: `
        ALTER TABLE biai.dataset_tables
        UPDATE
          row_count = {rowCount:UInt64},
          clickhouse_table_name = {tableName:String},
          schema_json = {schema:String}
        WHERE dataset_id = {datasetId:String} AND table_id = {tableId:String}
      `,
      query_params: {
        datasetId,
        tableId: table.table_id,
        rowCount: table.row_count,
        tableName: table.clickhouse_table_name,
        schema: table.schema_json
      }
    })
  }

  private async syncConnectedColumns(
    datasetId: string,
    tableId: string,
    columns: Array<{ name: string; type: string; nullable: boolean; position: number }>
  ): Promise<void> {
    if (!datasetId || !columns || columns.length === 0) {
      return
    }

    const existingResult = await clickhouseClient.query({
      query: `
        SELECT column_name
        FROM biai.dataset_columns
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
      `,
      query_params: { datasetId, tableId },
      format: 'JSONEachRow'
    })

    const existingRows = await existingResult.json<{ column_name: string }>()
    const existingNames = new Set(existingRows.map(row => row.column_name))

    const values = columns
      .filter(col => !existingNames.has(col.name))
      .map(col => {
        const displayType = this.inferDisplayType(col.type, col.name)
        return {
          dataset_id: datasetId,
          table_id: tableId,
          column_name: col.name,
          column_type: col.type,
          column_index: col.position,
          is_nullable: col.nullable,
          display_name: col.name,
          description: '',
          user_data_type: '',
          user_priority: null,
          display_type: displayType,
          unique_value_count: 0,
          null_count: 0,
          min_value: null,
          max_value: null,
          suggested_chart: displayType === 'categorical' ? 'bar' : 'histogram',
          display_priority: displayType === 'categorical' ? 50 : 0,
          is_hidden: false
        }
      })

    if (values.length > 0) {
      await clickhouseClient.insert({
        table: 'biai.dataset_columns',
        values,
        format: 'JSONEachRow'
      })
    }
  }

  async updatePrimaryKey(datasetId: string, tableId: string, primaryKey: string | null): Promise<void> {
    await clickhouseClient.command({
      query: `
        ALTER TABLE biai.dataset_tables
        UPDATE primary_key = {primaryKey:Nullable(String)}
        WHERE dataset_id = {datasetId:String} AND table_id = {tableId:String}
      `,
      query_params: {
        datasetId,
        tableId,
        primaryKey: primaryKey ?? null
      }
    })

    if (primaryKey) {
      const columnResult = await clickhouseClient.query({
        query: `
          SELECT *
          FROM biai.dataset_columns
          WHERE dataset_id = {datasetId:String}
            AND table_id = {tableId:String}
            AND column_name = {columnName:String}
          ORDER BY created_at DESC
          LIMIT 1
        `,
        query_params: { datasetId, tableId, columnName: primaryKey },
        format: 'JSONEachRow'
      })

      const rows = await columnResult.json<Record<string, any>>()
      if (rows.length > 0) {
        const current = rows[0]
        const updatedRow = {
          ...current,
          display_type: 'id',
          is_hidden: false,
          display_priority: 0,
          suggested_chart: 'none',
          created_at: Math.floor(Date.now() / 1000)
        }

        await clickhouseClient.insert({
          table: 'biai.dataset_columns',
          values: [updatedRow],
          format: 'JSONEachRow'
        })
      }
    }
  }

  async addRelationship(
    datasetId: string,
    tableId: string,
    relationship: { foreign_key: string; referenced_table: string; referenced_column: string; type?: string }
  ): Promise<void> {
    if (!relationship.foreign_key || !relationship.referenced_table || !relationship.referenced_column) {
      throw new Error('Relationship must include foreign_key, referenced_table, and referenced_column')
    }

    const existingResult = await clickhouseClient.query({
      query: `
        SELECT 1
        FROM biai.table_relationships
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
          AND foreign_key = {foreignKey:String}
          AND referenced_table = {referencedTable:String}
          AND referenced_column = {referencedColumn:String}
        LIMIT 1
      `,
      query_params: {
        datasetId,
        tableId,
        foreignKey: relationship.foreign_key,
        referencedTable: relationship.referenced_table,
        referencedColumn: relationship.referenced_column
      },
      format: 'JSONEachRow'
    })

    const exists = (await existingResult.json()).length > 0
    if (exists) return

    await clickhouseClient.insert({
      table: 'biai.table_relationships',
      values: [{
        dataset_id: datasetId,
        table_id: tableId,
        foreign_key: relationship.foreign_key,
        referenced_table: relationship.referenced_table,
        referenced_column: relationship.referenced_column,
        relationship_type: relationship.type || 'many-to-one'
      }],
      format: 'JSONEachRow'
    })
  }

  async deleteRelationship(
    datasetId: string,
    tableId: string,
    relationship: { foreign_key: string; referenced_table: string; referenced_column: string }
  ): Promise<void> {
    await clickhouseClient.command({
      query: `
        ALTER TABLE biai.table_relationships
        DELETE WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
          AND foreign_key = {foreignKey:String}
          AND referenced_table = {referencedTable:String}
          AND referenced_column = {referencedColumn:String}
      `,
      query_params: {
        datasetId,
        tableId,
        foreignKey: relationship.foreign_key,
        referencedTable: relationship.referenced_table,
        referencedColumn: relationship.referenced_column
      }
    })
  }

  private updateCustomMetadata(dataset: Dataset, updates: Record<string, any>) {
    let metadata: Record<string, any> = {}
    if (dataset.custom_metadata) {
      try {
        metadata = JSON.parse(dataset.custom_metadata)
      } catch {
        metadata = {}
      }
    }
    dataset.custom_metadata = JSON.stringify({ ...metadata, ...updates })
  }

  private async getStoredColumnMetadata(datasetId: string, tableId: string): Promise<any[]> {
    const storedResult = await clickhouseClient.query({
      query: `
        SELECT
          column_name,
          column_type,
          column_index,
          is_nullable,
          display_name,
          description,
          user_data_type,
          user_priority,
          display_type,
          unique_value_count,
          null_count,
          min_value,
          max_value,
          suggested_chart,
          display_priority,
          is_hidden
        FROM biai.dataset_columns
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
        ORDER BY column_index, created_at DESC
        LIMIT 1 BY column_name
      `,
      query_params: { datasetId, tableId },
      format: 'JSONEachRow'
    })

    const rows = await storedResult.json<{
      column_name: string
      column_type: string
      column_index: number
      is_nullable: number | boolean
      display_name: string
      description: string
      user_data_type: string
      user_priority: number | null
      display_type: string
      unique_value_count: number | null
      null_count: number | null
      min_value: string | null
      max_value: string | null
      suggested_chart: string | null
      display_priority: number | null
      is_hidden: number | boolean
    }>()

    return rows.map((row, index) => ({
      column_name: row.column_name,
      column_type: row.column_type,
      column_index: row.column_index ?? index,
      is_nullable: Boolean(row.is_nullable),
      display_name: row.display_name || row.column_name,
      description: row.description || '',
      user_data_type: row.user_data_type || '',
      user_priority: row.user_priority ?? null,
      display_type: this.normalizeDisplayType(row.display_type || this.inferDisplayType(row.column_type, row.column_name)),
      unique_value_count: row.unique_value_count ?? 0,
      null_count: row.null_count ?? 0,
      min_value: row.min_value ?? null,
      max_value: row.max_value ?? null,
      suggested_chart: row.suggested_chart || 'bar',
      display_priority: row.display_priority ?? 0,
      is_hidden: Boolean(row.is_hidden)
    }))
  }

  async updateColumnMetadata(
    datasetId: string,
    tableId: string,
    columnName: string,
    updates: { displayName?: string; description?: string; isHidden?: boolean; displayType?: string }
  ): Promise<void> {
    console.log('updateColumnMetadata called:', { datasetId, tableId, columnName, updates })

    // Fetch the existing column metadata
    const columnResult = await clickhouseClient.query({
      query: `
        SELECT *
        FROM biai.dataset_columns
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
          AND column_name = {columnName:String}
        ORDER BY created_at DESC
        LIMIT 1
      `,
      query_params: { datasetId, tableId, columnName },
      format: 'JSONEachRow'
    })

    const columns = await columnResult.json<any>()
    console.log('Found columns:', columns.length, columns.length > 0 ? columns[0] : null)

    if (columns.length === 0) {
      throw new Error('Column not found')
    }

    const existingColumn = columns[0]

    // Delete the old row
    await clickhouseClient.command({
      query: 'DELETE FROM biai.dataset_columns WHERE dataset_id = {datasetId:String} AND table_id = {tableId:String} AND column_name = {columnName:String}',
      query_params: { datasetId, tableId, columnName }
    })

    // Create updated row with merged data
    const updatedColumn = {
      ...existingColumn,
      display_name: updates.displayName !== undefined ? updates.displayName : existingColumn.display_name,
      description: updates.description !== undefined ? updates.description : existingColumn.description,
      is_hidden: updates.isHidden !== undefined ? updates.isHidden : existingColumn.is_hidden,
      display_type: updates.displayType !== undefined
        ? this.normalizeDisplayType(updates.displayType)
        : this.normalizeDisplayType(existingColumn.display_type)
    }
    // Remove created_at so it gets auto-generated by ClickHouse with current timestamp
    delete updatedColumn.created_at

    console.log('Inserting updated column:', updatedColumn)

    // Insert the updated row
    await clickhouseClient.insert({
      table: 'biai.dataset_columns',
      values: [updatedColumn],
      format: 'JSONEachRow'
    })

    console.log('Column metadata updated successfully')
  }

  private qualifyTableName(tableName: string): string {
    return tableName.includes('.') ? tableName : `biai.${tableName}`
  }

  private parseTableIdentifier(tableName: string): { database: string; table: string } {
    if (tableName.includes('.')) {
      const [database, table] = tableName.split('.', 2)
      return { database, table }
    }
    return { database: 'biai', table: tableName }
  }

  async deleteDataset(datasetId: string): Promise<void> {
    const dataset = await this.getDataset(datasetId)
    if (!dataset) {
      throw new Error('Dataset not found')
    }

    const tables = await this.getDatasetTables(datasetId)

    // Drop all tables for created datasets
    if (dataset.database_type === 'created') {
      for (const table of tables) {
        const { database, table: tableName } = this.parseTableIdentifier(table.clickhouse_table_name)
        // Escape identifiers for SQL safety
        await clickhouseClient.command({
          query: `DROP TABLE IF EXISTS ${escapeIdentifier(database)}.${escapeIdentifier(tableName)}`
        })
      }

      if (dataset.database_name) {
        await clickhouseClient.command({
          query: `DROP DATABASE IF EXISTS ${escapeIdentifier(dataset.database_name)}`
        })
      }
    }

    // Delete metadata
    await clickhouseClient.command({
      query: 'DELETE FROM biai.dataset_tables WHERE dataset_id = {datasetId:String}',
      query_params: { datasetId }
    })

    await clickhouseClient.command({
      query: 'DELETE FROM biai.dataset_columns WHERE dataset_id = {datasetId:String}',
      query_params: { datasetId }
    })

    await clickhouseClient.command({
      query: 'DELETE FROM biai.table_relationships WHERE dataset_id = {datasetId:String}',
      query_params: { datasetId }
    })

    await clickhouseClient.command({
      query: 'DELETE FROM biai.datasets_metadata WHERE dataset_id = {datasetId:String}',
      query_params: { datasetId }
    })
  }

  async deleteTable(datasetId: string, tableId: string): Promise<void> {
    // Get the dataset to find its database
    const dataset = await this.getDatasetMetadata(datasetId)
    if (!dataset || !dataset.database_name) {
      throw new Error('Dataset or database not found')
    }

    // Fetch table metadata from dataset_tables to get the actual table name
    const tableMetadataResult = await clickhouseClient.query({
      query: `
        SELECT table_name, clickhouse_table_name
        FROM biai.dataset_tables
        WHERE dataset_id = {datasetId:String}
          AND table_id = {tableId:String}
      `,
      query_params: { datasetId, tableId },
      format: 'JSONEachRow'
    })

    const tableMetadata = await tableMetadataResult.json<{ table_name: string; clickhouse_table_name: string }>()
    if (tableMetadata.length === 0) {
      throw new Error('Table not found in dataset')
    }

    const tableName = tableMetadata[0].table_name

    // Drop the table from the dataset's database
    // Escape identifiers for SQL safety
    await clickhouseClient.command({
      query: `DROP TABLE IF EXISTS ${escapeIdentifier(dataset.database_name)}.${escapeIdentifier(tableName)}`
    })

    // Delete table metadata from dataset_tables
    await clickhouseClient.command({
      query: 'DELETE FROM biai.dataset_tables WHERE dataset_id = {datasetId:String} AND table_id = {tableId:String}',
      query_params: { datasetId, tableId }
    })

    // Delete column metadata
    await clickhouseClient.command({
      query: 'DELETE FROM biai.dataset_columns WHERE dataset_id = {datasetId:String} AND table_id = {tableId:String}',
      query_params: { datasetId, tableId }
    })

    // Delete table relationships
    await clickhouseClient.command({
      query: 'DELETE FROM biai.table_relationships WHERE dataset_id = {datasetId:String} AND table_id = {tableId:String}',
      query_params: { datasetId, tableId }
    })
  }
}

export default new DatasetService()

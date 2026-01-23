import express from 'express'
import clickhouseClient, { createClickHouseClient } from '../config/clickhouse.js'
import datasetService from '../services/datasetService.js'
import aggregationService, { Filter } from '../services/aggregationService.js'
import { parseCountByQuery } from '../utils/countBy.js'
import { escapeIdentifier } from '../utils/sqlSanitizer.js'

const router = express.Router()

const parseStoredConnectionSettings = (raw?: string) => {
  if (!raw) return null
  try {
    const parsed = JSON.parse(raw)
    if (!parsed || !parsed.host) {
      return null
    }
    return parsed
  } catch (error) {
    console.warn('Failed to parse stored connection settings:', error)
    return null
  }
}

const buildAvailableDatabasesResponse = (rows: { name: string }[]) => {
  const excluded = new Set(['system', 'INFORMATION_SCHEMA', 'information_schema'])
  return rows
    .map(row => row.name)
    .filter(name => !excluded.has(name))
    .map(name => ({ name }))
}

const resolveClientForDataset = async (datasetId?: string) => {
  if (!datasetId) {
    return {
      client: clickhouseClient,
      shouldClose: false,
      databaseName: null as string | null
    }
  }

  const dataset = await datasetService.getDatasetMetadata(datasetId)
  if (!dataset) {
    const error: any = new Error('Dataset not found')
    error.status = 404
    throw error
  }

  if (dataset.database_type !== 'connected') {
    return {
      client: clickhouseClient,
      shouldClose: false,
      databaseName: dataset.database_name
    }
  }

  const connectionSettings = parseStoredConnectionSettings(dataset.connection_settings)
  if (!connectionSettings) {
    const error: any = new Error('No connection settings found for dataset')
    error.status = 400
    throw error
  }

  const client = createClickHouseClient({
    ...connectionSettings,
    database: dataset.database_name || connectionSettings.database || 'default'
  })

  return {
    client,
    shouldClose: true,
    databaseName: dataset.database_name || connectionSettings.database || null
  }
}

// List available databases (excluding system schemas)
router.get('/', async (_req, res) => {
  try {
    const result = await clickhouseClient.query({
      query: `
        SELECT name
        FROM system.databases
        ORDER BY name
      `,
      format: 'JSONEachRow'
    })

    const rows = await result.json<{ name: string }>()
    const databases = buildAvailableDatabasesResponse(rows)

    res.json({ databases })
  } catch (error: any) {
    console.error('List databases error:', error)
    res.status(500).json({ error: 'Failed to list databases', message: error.message })
  }
})

// List databases for a specific ClickHouse host provided by the client
router.post('/list', async (req, res) => {
  const {
    host,
    port,
    protocol,
    secure,
    username,
    password
  } = req.body || {}

  if (!host) {
    return res.status(400).json({ error: 'Host is required' })
  }

  const resolvedProtocol: 'http' | 'https' =
    protocol === 'https' || secure === true ? 'https' : 'http'

  const client = createClickHouseClient({
    host,
    port: port !== undefined && port !== null && port !== '' ? Number(port) : undefined,
    protocol: resolvedProtocol,
    username: username || undefined,
    password: password || undefined
  })

  try {
    const result = await client.query({
      query: `
        SELECT name
        FROM system.databases
        ORDER BY name
      `,
      format: 'JSONEachRow'
    })

    const rows = await result.json<{ name: string }>()
    res.json({ databases: buildAvailableDatabasesResponse(rows) })
  } catch (error: any) {
    console.error('List databases (custom host) error:', error)
    res.status(500).json({ error: 'Failed to list databases', message: error.message })
  } finally {
    await client.close()
  }
})

// Helper to infer display type from column type and name
function inferDisplayType(columnType: string, columnName: string): string {
  const nameLower = columnName.toLowerCase()

  // ID columns
  if (nameLower.includes('id') || nameLower.includes('key')) {
    return 'id'
  }

  // Numeric types
  if (columnType.includes('Int') || columnType.includes('Float') || columnType.includes('Decimal')) {
    return 'numeric'
  }

  // Date/DateTime
  if (columnType.includes('Date')) {
    return 'datetime'
  }

  // Everything else is categorical
  return 'categorical'
}

// Get database as if it were a dataset
router.get('/:database', async (req, res) => {
  try {
    const { database: databaseParam } = req.params
    const datasetId = req.query.datasetId as string | undefined

    const { client, shouldClose, databaseName } = await resolveClientForDataset(datasetId)
    const database = databaseName || databaseParam

    try {
      const tablesResult = await client.query({
        query: `
          SELECT name, engine, total_rows
          FROM system.tables
          WHERE database = {database:String}
            AND name NOT LIKE '.%'
          ORDER BY name
        `,
        query_params: { database },
        format: 'JSONEachRow'
      })

      const tables = await tablesResult.json<{ name: string; engine: string; total_rows: string }>()

      const tablesWithSchema = await Promise.all(
        tables.map(async (table) => {
          const columnsResult = await client.query({
            query: `
              SELECT name, type, position
              FROM system.columns
              WHERE database = {database:String}
                AND table = {table:String}
              ORDER BY position
            `,
            query_params: { database, table: table.name },
            format: 'JSONEachRow'
          })

          const columns = await columnsResult.json<{ name: string; type: string; position: number }>()

          return {
            id: table.name,
            name: table.name,
            displayName: table.name,
            rowCount: parseInt(table.total_rows) || 0,
            columns: columns.map(col => ({
              name: col.name,
              type: col.type.startsWith('Nullable(') && col.type.endsWith(')')
                ? col.type.slice(9, -1)
                : col.type,
              nullable: col.type.includes('Nullable')
            }))
          }
        })
      )

      res.json({
        dataset: {
          id: database,
          name: database,
          description: `ClickHouse database: ${database}`,
          tables: tablesWithSchema
        }
      })
    } finally {
      if (shouldClose) {
        await client.close()
      }
    }
  } catch (error: any) {
    const status = error?.status || 500
    console.error('Get database error:', error)
    res.status(status).json({
      error: status === 404 ? 'Dataset not found' : 'Failed to get database',
      message: error.message
    })
  }
})

// Get column metadata for a table (auto-generated)
router.get('/:database/tables/:table/columns', async (req, res) => {
  try {
    const { database: databaseParam, table } = req.params
    const datasetId = req.query.datasetId as string | undefined

    const { client, shouldClose, databaseName } = await resolveClientForDataset(datasetId)
    const database = databaseName || databaseParam

    try {
      const columnsResult = await client.query({
        query: `
          SELECT name, type, position
          FROM system.columns
          WHERE database = {database:String}
            AND table = {table:String}
          ORDER BY position
        `,
        query_params: { database, table },
        format: 'JSONEachRow'
      })

      const columns = await columnsResult.json<{ name: string; type: string; position: number }>()

      const columnMetadata = columns.map((col, index) => {
        const baseType = col.type.startsWith('Nullable(') && col.type.endsWith(')')
          ? col.type.slice(9, -1)
          : col.type
        const displayType = inferDisplayType(baseType, col.name)

        return {
          column_name: col.name,
          column_type: baseType,
          column_index: index,
          is_nullable: col.type.includes('Nullable'),
          display_name: col.name,
          description: '',
          user_data_type: '',
          user_priority: null,
          display_type: displayType,
          unique_value_count: 0,
          null_count: 0,
          min_value: null,
          max_value: null,
          suggested_chart: displayType === 'numeric' ? 'histogram' : 'bar',
          display_priority: 50,
          is_hidden: false
        }
      })

      res.json({ columns: columnMetadata })
    } finally {
      if (shouldClose) {
        await client.close()
      }
    }
  } catch (error: any) {
    const status = error?.status || 500
    console.error('Get columns error:', error)
    res.status(status).json({
      error: status === 404 ? 'Dataset not found' : 'Failed to get columns',
      message: error.message
    })
  }
})

// Get aggregations for a table (uses the column metadata we generated)
router.get('/:database/tables/:table/aggregations', async (req, res) => {
  try {
    const { database: databaseParam, table } = req.params
    const datasetId = req.query.datasetId as string | undefined
    let filters: Filter[] = []

    if (req.query.filters) {
      try {
        const parsed = JSON.parse(req.query.filters as string)
        filters = parsed
      } catch (error) {
        return res.status(400).json({ error: 'Invalid filters JSON' })
      }
    }

    const rawCountBy = typeof req.query.countBy === 'string' ? req.query.countBy : undefined
    const { config: countByConfig, error: countByError } = parseCountByQuery(rawCountBy)
    if (countByError) {
      return res.status(400).json({ error: countByError })
    }

    if (!datasetId && countByConfig) {
      return res.status(400).json({ error: 'countBy requires datasetId parameter' })
    }

    if (datasetId) {
      try {
        const aggregations = await aggregationService.getTableAggregations(datasetId, table, filters, countByConfig)
        return res.json({ aggregations })
      } catch (error: any) {
        const status = error?.status || 500
        console.error('Get database aggregations error:', error)
        return res.status(status).json({
          error: status === 400 ? 'Invalid countBy parameter' : 'Failed to get table aggregations',
          message: error.message
        })
      }
    }

    const { client, shouldClose, databaseName } = await resolveClientForDataset(datasetId)
    const database = databaseName || databaseParam

    try {
      const columnsResult = await client.query({
        query: `
          SELECT name, type
          FROM system.columns
          WHERE database = {database:String}
            AND table = {table:String}
          ORDER BY position
        `,
        query_params: { database, table },
        format: 'JSONEachRow'
      })

      const columns = await columnsResult.json<{ name: string; type: string }>()

      const aggregations = await Promise.all(
        columns.map(async (col) => {
          const baseType = col.type.startsWith('Nullable(') && col.type.endsWith(')')
            ? col.type.slice(9, -1)
            : col.type
          const displayType = inferDisplayType(baseType, col.name)

          // Escape identifiers for SQL safety
          const escapedCol = escapeIdentifier(col.name)
          const escapedTable = `${escapeIdentifier(database)}.${escapeIdentifier(table)}`

          const statsQuery = `
            SELECT
              count() as total_rows,
              countIf(isNull(${escapedCol})) as null_count,
              uniqExact(${escapedCol}) as unique_count
            FROM ${escapedTable}
          `

          const statsResult = await client.query({
            query: statsQuery,
            format: 'JSONEachRow'
          })

          const stats = await statsResult.json<{ total_rows: number; null_count: number; unique_count: number }>()
          const { total_rows, null_count, unique_count } = stats[0]

          const aggregation: any = {
            column_name: col.name,
            display_type: displayType,
            total_rows,
            null_count,
            unique_count
          }

          if (displayType === 'categorical' || displayType === 'id') {
            const categoriesQuery = `
              SELECT
                toString(${escapedCol}) as value,
                toString(${escapedCol}) as display_value,
                count() as count,
                count() * 100.0 / ${total_rows} as percentage
              FROM ${escapedTable}
              WHERE ${escapedCol} IS NOT NULL
              GROUP BY ${escapedCol}
              ORDER BY count DESC
              LIMIT 50
            `

            const categoriesResult = await client.query({
              query: categoriesQuery,
              format: 'JSONEachRow'
            })

            aggregation.categories = await categoriesResult.json()
          }

          if (displayType === 'numeric') {
            const numericQuery = `
              SELECT
                min(${escapedCol}) as min,
                max(${escapedCol}) as max,
                avg(${escapedCol}) as mean,
                median(${escapedCol}) as median,
                stddevPop(${escapedCol}) as stddev,
                quantile(0.25)(${escapedCol}) as q25,
                quantile(0.75)(${escapedCol}) as q75
              FROM ${escapedTable}
              WHERE ${escapedCol} IS NOT NULL
            `

            const numericResult = await client.query({
              query: numericQuery,
              format: 'JSONEachRow'
            })

            const numericStats = await numericResult.json()
            aggregation.numeric_stats = numericStats[0]

            const min = numericStats[0].min
            const max = numericStats[0].max

            if (min !== null && max !== null && min !== max) {
              const binWidth = (max - min) / 20

              const histogramQuery = `
                SELECT
                  ${min} + floor((${escapedCol} - ${min}) / ${binWidth}) * ${binWidth} as bin_start,
                  ${min} + (floor((${escapedCol} - ${min}) / ${binWidth}) + 1) * ${binWidth} as bin_end,
                  count() as count,
                  count() * 100.0 / ${total_rows} as percentage
                FROM ${escapedTable}
                WHERE ${escapedCol} IS NOT NULL
                GROUP BY bin_start, bin_end
                ORDER BY bin_start
              `

              const histogramResult = await client.query({
                query: histogramQuery,
                format: 'JSONEachRow'
              })

              aggregation.histogram = await histogramResult.json()
            }
          }

          return aggregation
        })
      )

      res.json({ aggregations })
    } finally {
      if (shouldClose) {
        await client.close()
      }
    }
  } catch (error: any) {
    const status = error?.status || 500
    console.error('Get aggregations error:', error)
    res.status(status).json({
      error: status === 404 ? 'Dataset not found' : 'Failed to get aggregations',
      message: error.message
    })
  }
})

export default router

import { describe, test, expect, beforeEach, afterEach, vi } from 'vitest'

const {
  commandMock,
  insertMock,
  queryMock,
  externalClientQueryMock,
  externalClientCloseMock,
  createClientMock
} = vi.hoisted(() => ({
  commandMock: vi.fn(),
  insertMock: vi.fn(),
  queryMock: vi.fn(),
  externalClientQueryMock: vi.fn(),
  externalClientCloseMock: vi.fn(),
  createClientMock: vi.fn()
}))

vi.mock('../../config/clickhouse.js', () => ({
  default: {
    command: commandMock,
    insert: insertMock,
    query: queryMock
  },
  createClickHouseClient: createClientMock.mockImplementation(() => ({
    query: externalClientQueryMock,
    close: externalClientCloseMock
  }))
}))

vi.mock('../columnAnalyzer.js', () => ({
  analyzeColumn: vi.fn()
}))

import datasetService from '../datasetService'
import { analyzeColumn } from '../columnAnalyzer.js'

const mockAnalyzeColumn = vi.mocked(analyzeColumn)

describe('DatasetService', () => {
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    commandMock.mockReset()
    insertMock.mockReset()
    queryMock.mockReset()
    mockAnalyzeColumn.mockReset()
    externalClientQueryMock.mockReset()
    externalClientCloseMock.mockReset()
    createClientMock.mockClear()
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(() => {
    consoleWarnSpy.mockRestore()
  })

  test('addTableToDataset stores metadata and analyzes columns using fully qualified table names', async () => {
    const datasetId = 'dataset-1'
    const datasetMeta = {
      dataset_id: datasetId,
      dataset_name: 'Glioblastoma',
      database_name: 'ds_glioblastoma_1234',
      database_type: 'created',
      description: 'Test dataset',
      tags: [],
      source: '',
      citation: '',
      references: [],
      custom_metadata: '{}',
      connection_settings: '',
      created_by: 'system',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }

    // getDataset -> datasets_metadata
    queryMock.mockResolvedValueOnce({
      json: async () => [datasetMeta]
    } as any)
    // getDatasetTables -> dataset_tables
    queryMock.mockResolvedValueOnce({
      json: async () => []
    } as any)
    // Row count query after data insert
    queryMock.mockResolvedValueOnce({
      json: async () => [{ cnt: '2' }]
    } as any)

    mockAnalyzeColumn.mockResolvedValue({
      display_type: 'id',
      unique_value_count: 2,
      null_count: 0,
      min_value: null,
      max_value: null,
      suggested_chart: 'none',
      display_priority: 0,
      is_hidden: true
    })

    insertMock.mockResolvedValue(undefined)
    commandMock.mockResolvedValue(undefined)

    const parsedData = {
      columns: [
        {
          name: 'patient_id',
          type: 'String',
          index: 0,
          nullable: false,
          displayName: 'Patient ID'
        }
      ],
      rows: [
        ['GBM-1'],
        ['GBM-2']
      ]
    }

    const result = await datasetService.addTableToDataset(
      datasetId,
      'patients',
      'Clinical Patients',
      'patients.tsv',
      'text/tab-separated-values',
      parsedData as any,
      'patient_id'
    )

    expect(result.table_name).toBe('patients')
    expect(result.display_name).toBe('Clinical Patients')
    expect(result.row_count).toBe(2)

    // CREATE TABLE command issued inside dataset database
    expect(commandMock.mock.calls[0]?.[0].query).toContain(`CREATE TABLE IF NOT EXISTS ${datasetMeta.database_name}.`)
    // Dataset timestamp update
    expect(commandMock.mock.calls[1]?.[0].query).toContain('ALTER TABLE biai.datasets_metadata UPDATE')

    // First insert should stream data into the dataset-specific table
    const dataInsertCall = insertMock.mock.calls.find(call => (call[0] as any).table.startsWith(`${datasetMeta.database_name}.`))
    expect(dataInsertCall).toBeTruthy()

    // Metadata rows stored in dataset_tables
    const tableMetadataCall = insertMock.mock.calls.find(call => (call[0] as any).table === 'biai.dataset_tables')
    expect(tableMetadataCall).toBeTruthy()
    const insertedTableMetadata = (tableMetadataCall![0] as any).values[0]
    expect(insertedTableMetadata.dataset_id).toBe(datasetId)
    expect(insertedTableMetadata.clickhouse_table_name.startsWith(`${datasetMeta.database_name}.`)).toBe(true)

    // Column metadata persisted
    const columnMetadataCall = insertMock.mock.calls.find(call => (call[0] as any).table === 'biai.dataset_columns')
    expect(columnMetadataCall).toBeTruthy()
    expect((columnMetadataCall![0] as any).values).toHaveLength(1)

    // analyzeColumn invoked with the fully-qualified table identifier
    expect(mockAnalyzeColumn).toHaveBeenCalledWith(
      insertedTableMetadata.clickhouse_table_name,
      'patient_id',
      'String'
    )

    // Service response mirrors stored metadata
    expect(result.table_id).toBe(insertedTableMetadata.table_id)
    expect(result.clickhouse_table_name).toBe(insertedTableMetadata.clickhouse_table_name)
  })

  describe('getTableData', () => {
    test('uses stored qualified table name when present', async () => {
      queryMock.mockResolvedValueOnce({
        json: async () => [{
          dataset_id: 'dataset-1',
          dataset_name: 'Test',
          database_name: 'customdb',
          database_type: 'created',
          connection_settings: ''
        }]
      } as any)
      queryMock.mockResolvedValueOnce({
        json: async () => [{
          dataset_id: 'dataset-1',
          table_id: 'patients',
          table_name: 'patients',
          display_name: 'patients',
          row_count: 100,
          clickhouse_table_name: 'customdb.tbl_patients',
          schema_json: '[]',
          created_at: new Date().toISOString()
        }]
      } as any)
      queryMock.mockResolvedValueOnce({
        json: async () => []
      } as any)
      queryMock.mockResolvedValueOnce({
        json: async () => [{ patient_id: 'GBM-1' }]
      } as any)

      const rows = await datasetService.getTableData('dataset-1', 'patients', 25, 5)

      expect(rows).toEqual([{ patient_id: 'GBM-1' }])
      expect(queryMock).toHaveBeenCalledTimes(4)
      expect(queryMock.mock.calls[3]?.[0].query).toContain('FROM customdb.tbl_patients')
      expect(queryMock.mock.calls[3]?.[0].query).toContain('LIMIT 25 OFFSET 5')
    })

    test('defaults to biai schema when table name is unqualified', async () => {
      queryMock.mockResolvedValueOnce({
        json: async () => [{
          dataset_id: 'dataset-1',
          dataset_name: 'Test',
          database_name: 'biai',
          database_type: 'created',
          connection_settings: ''
        }]
      } as any)
      queryMock.mockResolvedValueOnce({
        json: async () => [{
          dataset_id: 'dataset-1',
          table_id: 'patients',
          table_name: 'patients',
          display_name: 'patients',
          row_count: 100,
          clickhouse_table_name: 'tbl_patients',
          schema_json: '[]',
          created_at: new Date().toISOString()
        }]
      } as any)
      queryMock.mockResolvedValueOnce({
        json: async () => []
      } as any)
      queryMock.mockResolvedValueOnce({
        json: async () => []
      } as any)

      await datasetService.getTableData('dataset-1', 'patients', 10, 0)

      expect(queryMock.mock.calls[3]?.[0].query).toContain('FROM biai.tbl_patients')
    })
  })

  test('getTableColumns returns stored metadata for created datasets', async () => {
    // getDatasetMetadata query
    queryMock.mockResolvedValueOnce({
      json: async () => [{
        dataset_id: 'dataset-1',
        dataset_name: 'Test Dataset',
        database_name: 'biai',
        database_type: 'created',
        description: '',
        tags: [],
        source: '',
        citation: '',
        references: [],
        custom_metadata: '{}',
        connection_settings: '',
        created_by: 'system',
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }]
    } as any)

    // getDatasetTables query (called by getDataset for created datasets)
    queryMock.mockResolvedValueOnce({
      json: async () => [{
        dataset_id: 'dataset-1',
        table_id: 'cases',
        table_name: 'cases',
        display_name: 'Cases',
        row_count: 100,
        clickhouse_table_name: 'biai.cases',
        schema_json: '[]',
        primary_key: null,
        custom_metadata: '{}',
        created_at: new Date().toISOString()
      }]
    } as any)

    // getDatasetTables relationships query
    queryMock.mockResolvedValueOnce({ json: async () => [] } as any)

    // getStoredColumnMetadata query
    queryMock.mockResolvedValueOnce({
      json: async () => [
        {
          column_name: 'patient_id',
          column_type: 'String',
          column_index: 0,
          is_nullable: true,
          display_name: 'Patient ID',
          description: 'Patient identifier',
          user_data_type: '',
          user_priority: null,
          display_type: 'id',
          unique_value_count: 100,
          null_count: 0,
          min_value: null,
          max_value: null,
          suggested_chart: 'none',
          display_priority: 0,
          is_hidden: false
        },
        {
          column_name: 'age',
          column_type: 'UInt8',
          column_index: 1,
          is_nullable: false,
          display_name: 'Age',
          description: '',
          user_data_type: '',
          user_priority: null,
          display_type: 'numeric',
          unique_value_count: 0,
          null_count: 0,
          min_value: null,
          max_value: null,
          suggested_chart: 'histogram',
          display_priority: 0,
          is_hidden: false
        }
      ]
    } as any)

    const columns = await datasetService.getTableColumns('dataset-1', 'cases')

    // Should not call external client for created datasets
    expect(createClientMock).not.toHaveBeenCalled()
    expect(externalClientQueryMock).not.toHaveBeenCalled()

    expect(columns).toHaveLength(2)
    expect(columns[0]).toMatchObject({ column_name: 'patient_id', display_name: 'Patient ID' })
    expect(columns[1]).toMatchObject({ column_name: 'age', display_name: 'Age' })
  })

  test('getTableColumns falls back to stored metadata when connection settings are missing', async () => {
    queryMock
      .mockResolvedValueOnce({
        json: async () => [{
          dataset_id: 'dataset-1',
          dataset_name: 'Test dataset',
          database_name: 'remote_db',
          database_type: 'connected',
          connection_settings: '',
          description: '',
          tags: [],
          custom_metadata: '{}'
        }]
      } as any)
      .mockResolvedValueOnce({
        json: async () => [{
          dataset_id: 'dataset-1',
          table_id: 'cases',
          table_name: 'cases',
          display_name: 'Cases',
          row_count: 10,
          clickhouse_table_name: 'remote_db.cases',
          schema_json: '[]',
          primary_key: null,
          custom_metadata: '{}',
          created_at: new Date().toISOString()
        }]
      } as any)
      .mockResolvedValueOnce({ json: async () => [] } as any)
      .mockResolvedValueOnce({
        json: async () => [{
          column_name: 'sample_id',
          column_type: 'String',
          column_index: 0,
          is_nullable: 0,
          display_name: 'Sample ID',
          description: '',
          user_data_type: '',
          user_priority: null,
          display_type: 'id',
          unique_value_count: 10,
          null_count: 0,
          min_value: null,
          max_value: null,
          suggested_chart: 'none',
          display_priority: 0,
          is_hidden: 0
        }]
      } as any)

    const columns = await datasetService.getTableColumns('dataset-1', 'cases')

    expect(columns).toEqual([
      expect.objectContaining({
        column_name: 'sample_id',
        display_name: 'Sample ID',
        display_type: 'id'
      })
    ])
    expect(createClientMock).not.toHaveBeenCalled()
  })

  test('getTableColumns returns empty array when no stored metadata exists', async () => {
    // getDatasetMetadata query
    queryMock.mockResolvedValueOnce({
      json: async () => [{
        dataset_id: 'dataset-1',
        dataset_name: 'Test dataset',
        database_name: 'biai',
        database_type: 'created',
        connection_settings: '',
        description: '',
        tags: [],
        custom_metadata: '{}'
      }]
    } as any)

    // getDatasetTables query
    queryMock.mockResolvedValueOnce({
      json: async () => [{
        dataset_id: 'dataset-1',
        table_id: 'cases',
        table_name: 'cases',
        display_name: 'Cases',
        row_count: 10,
        clickhouse_table_name: 'biai.cases',
        schema_json: '[]',
        primary_key: null,
        custom_metadata: '{}',
        created_at: new Date().toISOString()
      }]
    } as any)

    // getDatasetTables relationships query
    queryMock.mockResolvedValueOnce({ json: async () => [] } as any)

    // getStoredColumnMetadata returns empty
    queryMock.mockResolvedValueOnce({
      json: async () => []
    } as any)

    const columns = await datasetService.getTableColumns('dataset-1', 'cases')

    expect(columns).toEqual([])
    // Should not attempt external client connection for created datasets
    expect(createClientMock).not.toHaveBeenCalled()
  })

  test('updatePrimaryKey updates dataset metadata', async () => {
    commandMock.mockResolvedValue(undefined)
    queryMock.mockResolvedValueOnce({
      json: async () => [{
        dataset_id: 'dataset-1',
        table_id: 'table-1',
        column_name: 'id',
        column_type: 'String',
        column_index: 0,
        is_nullable: false,
        display_name: 'id',
        description: '',
        user_data_type: '',
        user_priority: null,
        display_type: 'categorical',
        unique_value_count: 0,
        null_count: 0,
        min_value: null,
        max_value: null,
        suggested_chart: 'bar',
        display_priority: 0,
        is_hidden: false
      }]
    } as any)
    insertMock.mockResolvedValue(undefined)

    await datasetService.updatePrimaryKey('dataset-1', 'table-1', 'id')

    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
      query: expect.stringContaining('ALTER TABLE biai.dataset_tables')
    }))
    const columnInsert = insertMock.mock.calls.find(call => (call[0] as any).table === 'biai.dataset_columns')
    expect(columnInsert).toBeTruthy()
  })

  test('addRelationship inserts new relationship', async () => {
    queryMock.mockResolvedValueOnce({ json: async () => [] } as any)
    insertMock.mockResolvedValue(undefined)

    await datasetService.addRelationship('dataset-1', 'table-1', {
      foreign_key: 'patient_id',
      referenced_table: 'patients',
      referenced_column: 'id'
    })

    const relationshipInsert = insertMock.mock.calls.find(call => (call[0] as any).table === 'biai.table_relationships')
    expect(relationshipInsert).toBeTruthy()
  })

  test('deleteRelationship removes existing relationship', async () => {
    commandMock.mockResolvedValue(undefined)

    await datasetService.deleteRelationship('dataset-1', 'table-1', {
      foreign_key: 'patient_id',
      referenced_table: 'patients',
      referenced_column: 'id'
    })

    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
      query: expect.stringContaining('ALTER TABLE biai.table_relationships')
    }))
  })
})

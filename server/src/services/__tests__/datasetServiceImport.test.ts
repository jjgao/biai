import { describe, test, expect, beforeEach, vi } from 'vitest'

const {
  commandMock,
  insertMock,
  queryMock,
  createClientMock
} = vi.hoisted(() => ({
  commandMock: vi.fn(),
  insertMock: vi.fn(),
  queryMock: vi.fn(),
  createClientMock: vi.fn()
}))

vi.mock('../../config/clickhouse.js', () => ({
  default: {
    command: commandMock,
    insert: insertMock,
    query: queryMock
  },
  createClickHouseClient: createClientMock
}))

vi.mock('../columnAnalyzer.js', () => ({
  analyzeColumn: vi.fn().mockResolvedValue({
    display_type: 'id',
    unique_value_count: 0,
    null_count: 0,
    min_value: null,
    max_value: null,
    suggested_chart: 'none',
    display_priority: 0,
    is_hidden: false
  })
}))

import datasetService from '../datasetService'

describe('DatasetService - Import into Existing Table', () => {
  const datasetId = 'dataset-1'
  const databaseName = 'ds_test'
  
  beforeEach(() => {
    vi.clearAllMocks()
    
    // Mock getDataset
    queryMock.mockImplementation((params) => {
      const query = params.query
      if (query.includes('datasets_metadata')) {
        return {
          json: async () => [{
            dataset_id: datasetId,
            dataset_name: 'Test',
            database_name: databaseName,
            database_type: 'created',
            custom_metadata: '{}'
          }]
        }
      }
      if (query.includes('dataset_tables')) {
        return {
          json: async () => [{
            table_id: 'existing_table',
            table_name: 'existing_table',
            clickhouse_table_name: `${databaseName}.existing_table`,
            row_count: 5,
            primary_key: 'id'
          }]
        }
      }
      if (query.includes('table_relationships')) {
        return { json: async () => [] }
      }
      if (query.includes('DESCRIBE TABLE')) {
        return {
          json: async () => [
            { name: 'id', type: 'String' },
            { name: 'val', type: 'Int32' }
          ]
        }
      }
      if (query.includes('SELECT count()')) {
        return {
          json: async () => [{ cnt: '10' }]
        }
      }
      return { json: async () => [] }
    })
  })

  test('should append data to existing table', async () => {
    // Mock existing table
    const targetTable = {
      table_id: 'existing_table',
      table_name: 'existing_table',
      clickhouse_table_name: `${databaseName}.existing_table`,
      primary_key: 'id'
    }
    
    const parsedData = {
      columns: [
        { name: 'id', type: 'String' },
        { name: 'val', type: 'Int32' }
      ],
      rows: [['new1', 10], ['new2', 20]],
      rowCount: 2
    }

    await datasetService.addTableToDataset(
      datasetId,
      'ignored',
      'ignored',
      'file.csv',
      'text/csv',
      parsedData as any,
      undefined,
      {},
      [],
      'append',
      'existing_table'
    )

    // Should NOT create table
    expect(commandMock).not.toHaveBeenCalledWith(expect.objectContaining({
      query: expect.stringContaining('CREATE TABLE')
    }))

    // Should Insert
    expect(insertMock).toHaveBeenCalledWith(expect.objectContaining({
      table: `${databaseName}.existing_table`
    }))
    
    // Should Update Row Count
    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
      query: expect.stringContaining('UPDATE row_count')
    }))
  })

  test('should handle replace mode (truncate)', async () => {
    const parsedData = {
        columns: [{ name: 'id', type: 'String' }],
        rows: [['new1']],
        rowCount: 1
    }

    await datasetService.addTableToDataset(
      datasetId,
      'ignored',
      'ignored',
      'file.csv',
      'text/csv',
      parsedData as any,
      undefined,
      {},
      [],
      'replace',
      'existing_table'
    )

    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
      query: expect.stringContaining('TRUNCATE TABLE')
    }))
    
    expect(insertMock).toHaveBeenCalled()
  })

  test('should handle upsert mode', async () => {
    const parsedData = {
        columns: [{ name: 'id', type: 'String' }],
        rows: [['u1']],
        rowCount: 1
    }

    await datasetService.addTableToDataset(
      datasetId,
      'ignored',
      'ignored',
      'file.csv',
      'text/csv',
      parsedData as any,
      undefined,
      {},
      [],
      'upsert',
      'existing_table'
    )

    // Should create temp table
    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
      query: expect.stringContaining('CREATE TABLE')
    }))
    
    // Should insert into temp table (we can't check table name exactly as it has UUID)
    expect(insertMock).toHaveBeenCalled()
    
    // Should DELETE FROM main WHERE pk IN temp
    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
        query: expect.stringContaining('DELETE WHERE `id` IN (SELECT `id` FROM')
    }))
    
    // Should INSERT INTO main SELECT * FROM temp
    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
        query: expect.stringContaining('INSERT INTO')
    }))
  })

  test('should add new columns via ALTER TABLE', async () => {
    const parsedData = {
        columns: [
            { name: 'id', type: 'String' },
            { name: 'new_col', type: 'String' } // New column
        ],
        rows: [['1', 'val']],
        rowCount: 1
    }

    await datasetService.addTableToDataset(
      datasetId,
      'ignored',
      'ignored',
      'file.csv',
      'text/csv',
      parsedData as any,
      undefined,
      {},
      [],
      'append',
      'existing_table'
    )

    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
        query: expect.stringContaining('ALTER TABLE')
    }))
    
    expect(commandMock).toHaveBeenCalledWith(expect.objectContaining({
        query: expect.stringContaining('ADD COLUMN `new_col` Nullable(String)')
    }))
  })
})

import { describe, test, expect, beforeEach, afterAll, vi } from 'vitest'
import request from 'supertest'
import express, { Request, Response, NextFunction } from 'express'

const { mockMulterBody } = vi.hoisted(() => ({
  mockMulterBody: { current: {} }
}))

// Mock fileParser
vi.mock('../../services/fileParser.js', () => ({
  parseCSVFile: vi.fn().mockResolvedValue({
    columns: [{ name: 'col1', type: 'String' }],
    rows: [['val1']],
    rowCount: 1
  }),
  detectSkipRows: vi.fn(),
  detectDelimiter: vi.fn()
}))

// Mock fs/promises
vi.mock('fs/promises', () => ({
  unlink: vi.fn().mockResolvedValue(undefined)
}))

// Mock multer
vi.mock('multer', () => {
  const multer = () => ({
    single: () => (req: Request, _res: Response, next: NextFunction) => {
      req.file = {
        path: 'mock/path/test.csv',
        originalname: 'test.csv',
        mimetype: 'text/csv'
      } as Express.Multer.File
      req.body = { ...req.body, ...mockMulterBody.current }
      next()
    }
  })
  multer.diskStorage = vi.fn()
  return { default: multer }
})

// Mock the services
vi.mock('../../services/datasetService.js', () => ({
  default: {
    createDataset: vi.fn(),
    addTableToDataset: vi.fn(),
    listDatasets: vi.fn(),
    getDataset: vi.fn(),
    connectDatabase: vi.fn(),
    getTableColumns: vi.fn(),
    updateColumnMetadata: vi.fn(),
    deleteDataset: vi.fn(),
    getDatasetTables: vi.fn(),
    updateTableMetadata: vi.fn(),
    updateDatasetTimestamp: vi.fn()
  }
}))

// Mock directoryImportService
vi.mock('../../services/directoryImportService.js', () => ({
  importFromDirectory: vi.fn()
}))

import datasetsRouter from '../datasets.js'
import datasetService from '../../services/datasetService.js'
import { importFromDirectory } from '../../services/directoryImportService.js'

const mockCreateDataset = vi.mocked(datasetService.createDataset)
const mockListDatasets = vi.mocked(datasetService.listDatasets)
const mockGetDataset = vi.mocked(datasetService.getDataset)
const mockConnectDatabase = vi.mocked(datasetService.connectDatabase)
const mockGetTableColumns = vi.mocked(datasetService.getTableColumns)
const mockUpdateColumnMetadata = vi.mocked(datasetService.updateColumnMetadata)
const mockDeleteDataset = vi.mocked(datasetService.deleteDataset)
const mockGetDatasetTables = vi.mocked(datasetService.getDatasetTables)
const mockUpdateTableMetadata = vi.mocked(datasetService.updateTableMetadata)
const mockUpdateDatasetTimestamp = vi.mocked(datasetService.updateDatasetTimestamp)

const app = express()
app.use(express.json())
app.use('/api/datasets', datasetsRouter)

describe('Datasets API Routes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('POST /api/datasets', () => {
    test('should create a new dataset', async () => {
      const mockDataset = {
        dataset_id: 'test-id',
        dataset_name: 'Test Dataset',
        description: 'Test description',
        tags: ['test'],
        created_at: new Date(),
        updated_at: new Date()
      }

      mockCreateDataset.mockResolvedValue(mockDataset as any)

      const response = await request(app)
        .post('/api/datasets')
        .send({
          name: 'Test Dataset',
          description: 'Test description',
          tags: ['test']
        })

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(response.body.dataset.name).toBe('Test Dataset')
      expect(mockCreateDataset).toHaveBeenCalledWith(
        'Test Dataset',
        'Test description',
        'system',
        ['test'],
        '',
        '',
        [],
        {}
      )
    })

    test('should return 400 if name is missing', async () => {
      const response = await request(app)
        .post('/api/datasets')
        .send({
          description: 'Test description'
        })

      expect(response.status).toBe(400)
      expect(response.body.error).toBe('Dataset name is required')
    })
  })

  describe('GET /api/datasets', () => {
    test('should list all datasets', async () => {
      const mockDatasets = [
        {
          dataset_id: '1',
          dataset_name: 'Dataset 1',
          description: 'Desc 1',
          tags: [],
          tables: [],
          created_at: new Date(),
          updated_at: new Date(),
          connection_settings: ''
        },
        {
          dataset_id: '2',
          dataset_name: 'Dataset 2',
          description: 'Desc 2',
          tags: [],
          tables: [],
          created_at: new Date(),
          updated_at: new Date(),
          connection_settings: ''
        }
      ]

      mockListDatasets.mockResolvedValue(mockDatasets as any)

      const response = await request(app).get('/api/datasets')

      expect(response.status).toBe(200)
      expect(response.body.datasets).toHaveLength(2)
      expect(response.body.datasets[0].name).toBe('Dataset 1')
    })
  })

  describe('GET /api/datasets/:id', () => {
    test('should get dataset by id', async () => {
      const mockDataset = {
        dataset_id: 'test-id',
        dataset_name: 'Test Dataset',
        description: 'Test description',
        tags: ['test'],
        tables: [],
        created_at: new Date(),
        updated_at: new Date(),
        connection_settings: ''
      }

      mockGetDataset.mockResolvedValue(mockDataset as any)

      const response = await request(app).get('/api/datasets/test-id')

      expect(response.status).toBe(200)
      expect(response.body.dataset.name).toBe('Test Dataset')
      expect(mockGetDataset).toHaveBeenCalledWith('test-id')
    })

    test('should return 404 if dataset not found', async () => {
      mockGetDataset.mockResolvedValue(null)

      const response = await request(app).get('/api/datasets/nonexistent')

      expect(response.status).toBe(404)
      expect(response.body.error).toBe('Dataset not found')
    })
  })

  describe('POST /api/datasets/connect', () => {
    test('should require host', async () => {
      const response = await request(app)
        .post('/api/datasets/connect')
        .send({
          databaseName: 'analytics',
          displayName: 'Analytics'
        })

      expect(response.status).toBe(400)
      expect(response.body.error).toBe('Database name, display name, and host are required')
    })

    test('should connect dataset with connection info', async () => {
      mockConnectDatabase.mockResolvedValue({
        dataset_id: 'connected-1',
        dataset_name: 'Analytics',
        database_name: 'analytics',
        database_type: 'connected',
        description: '',
        tags: [],
        connection_settings: JSON.stringify({
          host: 'remote.clickhouse.local',
          port: 8443,
          protocol: 'https',
          username: 'readonly',
          password: 'secret'
        }),
        created_at: new Date()
      } as any)

      const response = await request(app)
        .post('/api/datasets/connect')
        .send({
          databaseName: 'analytics',
          displayName: 'Analytics',
          host: 'remote.clickhouse.local',
          secure: true,
          port: 8443,
          username: 'readonly',
          password: 'secret'
        })

      expect(response.status).toBe(200)
      expect(mockConnectDatabase).toHaveBeenCalledWith(
        'analytics',
        'Analytics',
        '',
        'system',
        [],
        {},
        {
          host: 'remote.clickhouse.local',
          port: 8443,
          protocol: 'https',
          username: 'readonly',
          password: 'secret'
        }
      )
      expect(response.body.dataset.connectionInfo).toEqual({
        host: 'remote.clickhouse.local',
        port: 8443,
        protocol: 'https',
        username: 'readonly'
      })
    })
  })

  describe('POST /api/datasets/:id/tables', () => {
    // Mock for addTableToDataset is required for these tests
    const mockAddTableToDataset = vi.mocked(datasetService.addTableToDataset)

    beforeEach(() => {
      mockAddTableToDataset.mockReset()
      mockMulterBody.current = {}
    })

    test('should pass importMode=append and targetTableId to service', async () => {
      mockMulterBody.current = {
        tableName: 'existing_table',
        importMode: 'append',
        targetTableId: 'existing_table_id'
      }

      mockAddTableToDataset.mockResolvedValue({
        table_id: 'existing_table',
        table_name: 'existing_table',
        display_name: 'Existing Table',
        original_filename: 'test.csv',
        file_type: 'text/csv',
        row_count: 10,
        clickhouse_table_name: 'db.existing_table',
        schema_json: '[]',
        created_at: new Date()
      })

      const response = await request(app)
        .post('/api/datasets/test-ds/tables')
        .field('tableName', 'existing_table')
        .field('importMode', 'append')
        .field('targetTableId', 'existing_table_id')
        .attach('file', Buffer.from('col1\nval1'), 'test.csv')

      expect(response.status).toBe(200)
      expect(mockAddTableToDataset).toHaveBeenCalledWith(
        'test-ds',
        'existing_table',
        'existing_table',
        'test.csv',
        'text/csv',
        expect.anything(),
        undefined,
        {},
        [],
        'append',
        'existing_table_id'
      )
    })

    test('should pass importMode=replace and targetTableId to service', async () => {
      mockMulterBody.current = {
        tableName: 'existing_table',
        importMode: 'replace',
        targetTableId: 'existing_table_id'
      }

      mockAddTableToDataset.mockResolvedValue({
        table_id: 'existing_table',
        table_name: 'existing_table',
        display_name: 'Existing Table',
        original_filename: 'test.csv',
        file_type: 'text/csv',
        row_count: 10,
        clickhouse_table_name: 'db.existing_table',
        schema_json: '[]',
        created_at: new Date()
      })

      const response = await request(app)
        .post('/api/datasets/test-ds/tables')
        .field('tableName', 'existing_table')
        .field('importMode', 'replace')
        .field('targetTableId', 'existing_table_id')
        .attach('file', Buffer.from('col1\nval1'), 'test.csv')

      expect(response.status).toBe(200)
      expect(mockAddTableToDataset).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        undefined,
        {},
        [],
        'replace',
        'existing_table_id'
      )
    })

    test('should pass importMode=upsert and targetTableId to service', async () => {
      mockMulterBody.current = {
        tableName: 'existing_table',
        importMode: 'upsert',
        targetTableId: 'existing_table_id'
      }

      mockAddTableToDataset.mockResolvedValue({
        table_id: 'existing_table',
        table_name: 'existing_table',
        display_name: 'Existing Table',
        original_filename: 'test.csv',
        file_type: 'text/csv',
        row_count: 10,
        clickhouse_table_name: 'db.existing_table',
        schema_json: '[]',
        created_at: new Date()
      })

      const response = await request(app)
        .post('/api/datasets/test-ds/tables')
        .field('tableName', 'existing_table')
        .field('importMode', 'upsert')
        .field('targetTableId', 'existing_table_id')
        .attach('file', Buffer.from('col1\nval1'), 'test.csv')

      expect(response.status).toBe(200)
      expect(mockAddTableToDataset).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        undefined,
        {},
        [],
        'upsert',
        'existing_table_id'
      )
    })

    test('should default to importMode=append if missing', async () => {
      mockMulterBody.current = {
        tableName: 'new_table'
      }

      mockAddTableToDataset.mockResolvedValue({
        table_id: 'new_table',
        table_name: 'new_table',
        display_name: 'New Table',
        original_filename: 'test.csv',
        file_type: 'text/csv',
        row_count: 10,
        clickhouse_table_name: 'db.new_table',
        schema_json: '[]',
        created_at: new Date()
      })

      const response = await request(app)
        .post('/api/datasets/test-ds/tables')
        .field('tableName', 'new_table')
        .attach('file', Buffer.from('col1\nval1'), 'test.csv')

      expect(response.status).toBe(200)
      expect(mockAddTableToDataset).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        undefined,
        {},
        [],
        'append',
        undefined
      )
    })

    test('should return 400 for invalid importMode', async () => {
      mockMulterBody.current = {
        tableName: 'table',
        importMode: 'invalid_mode'
      }

      const response = await request(app)
        .post('/api/datasets/test-ds/tables')
        .field('tableName', 'table')
        .field('importMode', 'invalid_mode')
        .attach('file', Buffer.from('col1\nval1'), 'test.csv')

      expect(response.status).toBe(400)
      expect(response.body.error).toContain('Invalid importMode')
      expect(mockAddTableToDataset).not.toHaveBeenCalled()
    })
  })

  describe('GET /api/datasets/:id/tables/:tableId/columns', () => {
    test('should get table columns', async () => {
      const mockColumns = [
        {
          column_name: 'id',
          column_type: 'String',
          display_name: 'ID',
          description: 'Primary key',
          display_type: 'id'
        },
        {
          column_name: 'name',
          column_type: 'String',
          display_name: 'Name',
          description: 'Patient name',
          display_type: 'text'
        }
      ]

      mockGetTableColumns.mockResolvedValue(mockColumns as any)

      const response = await request(app)
        .get('/api/datasets/dataset-id/tables/table-id/columns')

      expect(response.status).toBe(200)
      expect(response.body.columns).toHaveLength(2)
      expect(response.body.columns[0].column_name).toBe('id')
      expect(mockGetTableColumns).toHaveBeenCalledWith('dataset-id', 'table-id')
    })
  })

  describe('PATCH /api/datasets/:id/tables/:tableId/columns/:columnName', () => {
    test('should update column metadata', async () => {
      mockUpdateColumnMetadata.mockResolvedValue(undefined)

      const response = await request(app)
        .patch('/api/datasets/dataset-id/tables/table-id/columns/age')
        .send({
          displayName: 'Patient Age',
          description: 'Age in years',
          isHidden: false
        })

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(mockUpdateColumnMetadata).toHaveBeenCalledWith(
        'dataset-id',
        'table-id',
        'age',
        {
          displayName: 'Patient Age',
          description: 'Age in years',
          isHidden: false,
          displayType: undefined
        }
      )
    })

    test('should update display type', async () => {
      mockUpdateColumnMetadata.mockResolvedValue(undefined)

      const response = await request(app)
        .patch('/api/datasets/dataset-id/tables/table-id/columns/status')
        .send({
          displayType: 'category'
        })

      expect(response.status).toBe(200)
      expect(mockUpdateColumnMetadata).toHaveBeenCalledWith(
        'dataset-id',
        'table-id',
        'status',
        expect.objectContaining({
          displayType: 'category'
        })
      )
    })
  })

  describe('DELETE /api/datasets/:id', () => {
    test('should delete dataset', async () => {
      mockDeleteDataset.mockResolvedValue(undefined)

      const response = await request(app).delete('/api/datasets/test-id')

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(response.body.message).toBe('Dataset deleted successfully')
      expect(mockDeleteDataset).toHaveBeenCalledWith('test-id')
    })
  })

  describe('POST /api/datasets/import-from-path', () => {
    const mockImportFromDirectory = vi.mocked(importFromDirectory)
    const originalEnv = { ...process.env }

    beforeEach(() => {
      mockImportFromDirectory.mockReset()
      delete process.env.BIAI_ENABLE_IMPORT_FROM_PATH
      delete process.env.BIAI_IMPORT_ALLOWED_PATHS
    })

    afterAll(() => {
      process.env = originalEnv
    })

    test('should return 403 when BIAI_ENABLE_IMPORT_FROM_PATH is not set', async () => {
      const response = await request(app)
        .post('/api/datasets/import-from-path')
        .send({ path: '/some/dir' })

      expect(response.status).toBe(403)
      expect(response.body.error).toContain('Directory import is disabled')
      expect(mockImportFromDirectory).not.toHaveBeenCalled()
    })

    test('should return 400 when path is missing', async () => {
      process.env.BIAI_ENABLE_IMPORT_FROM_PATH = 'true'

      const response = await request(app)
        .post('/api/datasets/import-from-path')
        .send({})

      expect(response.status).toBe(400)
      expect(response.body.error).toBe('path is required and must be a string')
    })

    test('should return 400 when path is not a string', async () => {
      process.env.BIAI_ENABLE_IMPORT_FROM_PATH = 'true'

      const response = await request(app)
        .post('/api/datasets/import-from-path')
        .send({ path: 123 })

      expect(response.status).toBe(400)
      expect(response.body.error).toBe('path is required and must be a string')
    })

    test('should return 403 when path is not in allowlist', async () => {
      process.env.BIAI_ENABLE_IMPORT_FROM_PATH = 'true'
      process.env.BIAI_IMPORT_ALLOWED_PATHS = '/allowed/path1,/allowed/path2'

      const response = await request(app)
        .post('/api/datasets/import-from-path')
        .send({ path: '/not/allowed/dir' })

      expect(response.status).toBe(403)
      expect(response.body.error).toBe('Path not in allowed import paths')
      expect(mockImportFromDirectory).not.toHaveBeenCalled()
    })

    test('should succeed when feature is enabled and no allowlist is set', async () => {
      process.env.BIAI_ENABLE_IMPORT_FROM_PATH = 'true'

      mockImportFromDirectory.mockResolvedValue({
        datasetId: 'ds-1',
        datasetName: 'Test Dataset',
        tables: [{ tableId: 't-1', tableName: 'patients', displayName: 'Patients', rowCount: 100 }]
      })

      const response = await request(app)
        .post('/api/datasets/import-from-path')
        .send({ path: '/some/dataset/dir' })

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(response.body.datasetId).toBe('ds-1')
      expect(response.body.datasetName).toBe('Test Dataset')
      expect(response.body.tables).toHaveLength(1)
      expect(mockImportFromDirectory).toHaveBeenCalled()
    })

    test('should succeed when path is within allowlist', async () => {
      process.env.BIAI_ENABLE_IMPORT_FROM_PATH = 'true'
      process.env.BIAI_IMPORT_ALLOWED_PATHS = '/allowed/base'

      mockImportFromDirectory.mockResolvedValue({
        datasetId: 'ds-2',
        datasetName: 'Allowed Dataset',
        tables: []
      })

      const response = await request(app)
        .post('/api/datasets/import-from-path')
        .send({ path: '/allowed/base/sub/dir' })

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(mockImportFromDirectory).toHaveBeenCalled()
    })

    test('should return 500 when importFromDirectory throws', async () => {
      process.env.BIAI_ENABLE_IMPORT_FROM_PATH = 'true'

      mockImportFromDirectory.mockRejectedValue(new Error('Directory not found'))

      const response = await request(app)
        .post('/api/datasets/import-from-path')
        .send({ path: '/nonexistent/dir' })

      expect(response.status).toBe(500)
      expect(response.body.error).toBe('Failed to import from path')
      expect(response.body.message).toBe('Directory not found')
    })
  })

  describe('PATCH /api/datasets/:id/tables/:tableId', () => {
    const datasetId = 'ds-1'
    const tableId = 'tbl-1'

    test('should rename table with valid displayName', async () => {
      mockGetDatasetTables.mockResolvedValue([{ table_id: tableId }] as any)
      mockUpdateTableMetadata.mockResolvedValue(undefined)
      mockUpdateDatasetTimestamp.mockResolvedValue(undefined)

      const response = await request(app)
        .patch(`/api/datasets/${datasetId}/tables/${tableId}`)
        .send({ displayName: 'New Name' })

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(mockUpdateTableMetadata).toHaveBeenCalledWith(datasetId, tableId, { displayName: 'New Name' })
      expect(mockUpdateDatasetTimestamp).toHaveBeenCalledWith(datasetId)
    })

    test('should return 400 for empty displayName', async () => {
      const response = await request(app)
        .patch(`/api/datasets/${datasetId}/tables/${tableId}`)
        .send({ displayName: '' })

      expect(response.status).toBe(400)
      expect(response.body.error).toBe('Display name must be a non-empty string')
    })

    test('should return 400 for whitespace-only displayName', async () => {
      const response = await request(app)
        .patch(`/api/datasets/${datasetId}/tables/${tableId}`)
        .send({ displayName: '   ' })

      expect(response.status).toBe(400)
      expect(response.body.error).toBe('Display name must be a non-empty string')
    })

    test('should return 400 for non-string displayName', async () => {
      const response = await request(app)
        .patch(`/api/datasets/${datasetId}/tables/${tableId}`)
        .send({ displayName: 123 })

      expect(response.status).toBe(400)
      expect(response.body.error).toBe('Display name must be a non-empty string')
    })

    test('should return 404 for non-existent table', async () => {
      mockGetDatasetTables.mockResolvedValue([])

      const response = await request(app)
        .patch(`/api/datasets/${datasetId}/tables/${tableId}`)
        .send({ displayName: 'New Name' })

      expect(response.status).toBe(404)
      expect(response.body.error).toBe('Table not found')
    })

    test('should return success when no fields to update', async () => {
      mockGetDatasetTables.mockResolvedValue([{ table_id: tableId }] as any)

      const response = await request(app)
        .patch(`/api/datasets/${datasetId}/tables/${tableId}`)
        .send({})

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(mockUpdateTableMetadata).not.toHaveBeenCalled()
    })

    test('should return 500 on service error', async () => {
      mockGetDatasetTables.mockResolvedValue([{ table_id: tableId }] as any)
      mockUpdateTableMetadata.mockRejectedValue(new Error('DB error'))

      const response = await request(app)
        .patch(`/api/datasets/${datasetId}/tables/${tableId}`)
        .send({ displayName: 'New Name' })

      expect(response.status).toBe(500)
      expect(response.body.error).toBe('Failed to update table metadata')
    })
  })

  describe('Error Handling', () => {
    test('should handle service errors gracefully', async () => {
      mockListDatasets.mockRejectedValue(new Error('Database error'))

      const response = await request(app).get('/api/datasets')

      expect(response.status).toBe(500)
      expect(response.body.error).toBe('Failed to list datasets')
    })
  })
})

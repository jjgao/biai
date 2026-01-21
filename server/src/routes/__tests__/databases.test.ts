import { describe, test, expect, beforeEach, vi } from 'vitest'
import request from 'supertest'
import express from 'express'

const { queryMock, closeMock, datasetMetadataMock, aggregationMock } = vi.hoisted(() => ({
  queryMock: vi.fn(),
  closeMock: vi.fn(),
  datasetMetadataMock: vi.fn(),
  aggregationMock: vi.fn()
}))

vi.mock('../../config/clickhouse.js', () => ({
  default: {
    query: queryMock
  },
  createClickHouseClient: vi.fn(() => ({
    query: queryMock,
    close: closeMock
  }))
}))

vi.mock('../../services/datasetService.js', () => ({
  default: {
    getDatasetMetadata: datasetMetadataMock
  }
}))

vi.mock('../../services/aggregationService.js', () => ({
  default: {
    getTableAggregations: aggregationMock
  }
}))

import databasesRouter from '../databases.js'

const app = express()
app.use(express.json())
app.use('/api/databases', databasesRouter)

describe('Databases API Routes', () => {
  beforeEach(() => {
    queryMock.mockReset()
    closeMock.mockReset()
    datasetMetadataMock.mockReset()
    aggregationMock.mockReset()
  })

  test('GET /api/databases returns non-system databases', async () => {
    queryMock.mockResolvedValue({
      json: async () => [
        { name: 'biai' },
        { name: 'system' },
        { name: 'analytics' },
        { name: 'INFORMATION_SCHEMA' }
      ]
    } as any)

    const response = await request(app).get('/api/databases')

    expect(response.status).toBe(200)
    expect(queryMock).toHaveBeenCalledTimes(1)
    expect(response.body.databases).toEqual([
      { name: 'biai' },
      { name: 'analytics' }
    ])
  })

  test('GET /api/databases handles ClickHouse errors', async () => {
    queryMock.mockRejectedValue(new Error('unavailable'))

    const response = await request(app).get('/api/databases')

    expect(response.status).toBe(500)
    expect(response.body.error).toBe('Failed to list databases')
  })

  test('POST /api/databases/list retrieves databases for custom host', async () => {
    queryMock.mockResolvedValueOnce({
      json: async () => [
        { name: 'analytics' },
        { name: 'system' }
      ]
    } as any)

    const response = await request(app)
      .post('/api/databases/list')
      .send({ host: 'remote.clickhouse.local', secure: true, username: 'readonly', password: 'secret' })

    expect(response.status).toBe(200)
    expect(response.body.databases).toEqual([{ name: 'analytics' }])
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  test('GET /api/databases/:db/tables/:table/aggregations delegates to dataset pipeline when datasetId provided', async () => {
    const filters = [{ column: 'gene', operator: 'eq', value: 'TP53', tableName: 'mutations' }]
    aggregationMock.mockResolvedValue([{ column_name: 'gene', display_type: 'categorical' }])

    const response = await request(app)
      .get('/api/databases/remote/tables/mutations/aggregations')
      .query({ datasetId: 'dataset-1', filters: JSON.stringify(filters) })

    expect(response.status).toBe(200)
    expect(aggregationMock).toHaveBeenCalledWith('dataset-1', 'mutations', filters, undefined)
    expect(response.body.aggregations).toEqual([{ column_name: 'gene', display_type: 'categorical' }])
    expect(closeMock).not.toHaveBeenCalled()
  })

  test('GET /api/databases/:db/tables/:table/aggregations returns 400 on invalid filter JSON', async () => {
    const response = await request(app)
      .get('/api/databases/remote/tables/mutations/aggregations')
      .query({ datasetId: 'dataset-1', filters: 'not-json' })

    expect(response.status).toBe(400)
    expect(response.body.error).toBe('Invalid filters JSON')
    expect(aggregationMock).not.toHaveBeenCalled()
  })
})

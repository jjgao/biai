import { describe, test, expect, beforeEach, vi } from 'vitest'
import request from 'supertest'
import express from 'express'

// Mock the services
vi.mock('../../services/spreadsheetParser.js', () => ({
  getSpreadsheetPreview: vi.fn(),
  parseSpreadsheetSheet: vi.fn()
}))

vi.mock('../../services/datasetService.js', () => ({
  default: {
    addTableToDataset: vi.fn(),
    getDataset: vi.fn()
  }
}))

vi.mock('../../utils/urlFetcher.js', () => ({
  fetchFileFromUrl: vi.fn()
}))

// Mock multer
vi.mock('multer', () => {
  const multer = () => ({
    single: () => (req: any, res: any, next: any) => {
      req.file = {
        path: 'mock/path/test.xlsx',
        originalname: 'test.xlsx',
        mimetype: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      }
      // In a real multipart request, multer would populate req.body
      // For testing, we can manually populate it if it's empty
      if (Object.keys(req.body).length === 0 && req.headers['content-type']?.includes('multipart/form-data')) {
          // This is a hack for the test
          // In a real scenario, we'd use a real multer or a better mock
      }
      next()
    }
  })
  multer.diskStorage = vi.fn()
  return { default: multer }
})

// Mock fs/promises
vi.mock('fs/promises', () => ({
  unlink: vi.fn().mockResolvedValue(undefined)
}))

import datasetsRouter from '../datasets.js'
import { getSpreadsheetPreview, parseSpreadsheetSheet } from '../../services/spreadsheetParser.js'
import datasetService from '../../services/datasetService.js'

const mockGetSpreadsheetPreview = vi.mocked(getSpreadsheetPreview)
const mockParseSpreadsheetSheet = vi.mocked(parseSpreadsheetSheet)
const mockAddTableToDataset = vi.mocked(datasetService.addTableToDataset)

const app = express()
app.use(express.json())
app.use('/api/datasets', datasetsRouter)

describe('Spreadsheet API Routes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('POST /api/datasets/:id/spreadsheets/preview', () => {
    test('should return spreadsheet preview', async () => {
      const mockPreview = {
        filename: 'test.xlsx',
        sheets: [
          { name: 'Sheet1', rowCount: 10, columns: ['A', 'B'] }
        ]
      }
      mockGetSpreadsheetPreview.mockResolvedValue(mockPreview)

      const response = await request(app)
        .post('/api/datasets/test-ds/spreadsheets/preview')
        .attach('file', Buffer.from('mock content'), 'test.xlsx')

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(response.body.preview).toEqual(mockPreview)
      expect(mockGetSpreadsheetPreview).toHaveBeenCalled()
    })
  })

  describe('POST /api/datasets/:id/spreadsheets/import', () => {
    test('should import selected sheets', async () => {
      const mockParsedData = {
        columns: [{ name: 'A', type: 'String', nullable: true, index: 0 }],
        rows: [['val']],
        rowCount: 1
      }
      mockParseSpreadsheetSheet.mockResolvedValue(mockParsedData as any)
      mockAddTableToDataset.mockResolvedValue({
        table_id: 'sheet1',
        table_name: 'sheet1',
        display_name: 'Sheet 1',
        row_count: 1
      } as any)

      const response = await request(app)
        .post('/api/datasets/test-ds/spreadsheets/import')
        .send({
          sheetsConfig: JSON.stringify([
            { sheetName: 'Sheet1', tableName: 'sheet1', displayName: 'Sheet 1' }
          ])
        })

      expect(response.status).toBe(200)
      expect(response.body.success).toBe(true)
      expect(response.body.importedTables).toHaveLength(1)
      expect(mockParseSpreadsheetSheet).toHaveBeenCalled()
      expect(mockAddTableToDataset).toHaveBeenCalled()
    })
  })
})
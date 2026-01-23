import { describe, test, expect, beforeAll, afterAll } from 'vitest'
import * as XLSX from 'xlsx'
import * as fs from 'fs'
import * as path from 'path'
import { getSpreadsheetPreview, parseSpreadsheetSheet } from '../spreadsheetParser.js'

describe('spreadsheetParser', () => {
  const uploadsDir = path.join(process.cwd(), 'uploads')
  const testFilePath = path.join(uploadsDir, 'test_spreadsheet.xlsx')

  beforeAll(() => {
    // Ensure uploads directory exists
    if (!fs.existsSync(uploadsDir)) {
      fs.mkdirSync(uploadsDir, { recursive: true })
    }

    // Create a sample workbook
    const wb = XLSX.utils.book_new()
    
    // Sheet 1: Patients
    const ws1Data = [
      ['patient_id', 'age', 'gender'],
      ['P1', 45, 'M'],
      ['P2', 32, 'F'],
      ['P3', 67, 'M']
    ]
    const ws1 = XLSX.utils.aoa_to_sheet(ws1Data)
    XLSX.utils.book_append_sheet(wb, ws1, 'Patients')
    
    // Sheet 2: Samples
    const ws2Data = [
      ['sample_id', 'patient_id', 'type'],
      ['S1', 'P1', 'Tumor'],
      ['S2', 'P1', 'Normal'],
      ['S3', 'P2', 'Tumor']
    ]
    const ws2 = XLSX.utils.aoa_to_sheet(ws2Data)
    XLSX.utils.book_append_sheet(wb, ws2, 'Samples')
    
    // Write the file
    XLSX.writeFile(wb, testFilePath)
  })

  afterAll(() => {
    if (fs.existsSync(testFilePath)) {
      fs.unlinkSync(testFilePath)
    }
  })

  test('getSpreadsheetPreview returns correct info', async () => {
    const preview = await getSpreadsheetPreview(testFilePath)
    
    expect(preview.filename).toBe('test_spreadsheet.xlsx')
    expect(preview.sheets).toHaveLength(2)
    
    expect(preview.sheets[0].name).toBe('Patients')
    expect(preview.sheets[0].rowCount).toBe(4)
    expect(preview.sheets[0].columns).toEqual(['patient_id', 'age', 'gender'])
    
    expect(preview.sheets[1].name).toBe('Samples')
    expect(preview.sheets[1].rowCount).toBe(4)
    expect(preview.sheets[1].columns).toEqual(['sample_id', 'patient_id', 'type'])
  })

  test('parseSpreadsheetSheet parses data correctly', async () => {
    const parsed = await parseSpreadsheetSheet(testFilePath, 'Patients')
    
    expect(parsed.rowCount).toBe(3)
    expect(parsed.columns).toHaveLength(3)
    expect(parsed.columns[0].name).toBe('patient_id')
    expect(parsed.columns[1].name).toBe('age')
    expect(parsed.columns[1].type).toBe('Int32')
    
    expect(parsed.rows).toHaveLength(3)
    expect(parsed.rows[0]).toEqual(['P1', 45, 'M'])
  })

  test('parseSpreadsheetSheet handles skipRows', async () => {
    // Re-create workbook with metadata row
    const wb = XLSX.utils.book_new()
    const wsData = [
      ['# Metadata info'],
      ['patient_id', 'age'],
      ['P1', 45]
    ]
    const ws = XLSX.utils.aoa_to_sheet(wsData)
    XLSX.utils.book_append_sheet(wb, ws, 'Data')
    XLSX.writeFile(wb, testFilePath)
    
    const parsed = await parseSpreadsheetSheet(testFilePath, 'Data', 1)
    
    expect(parsed.rowCount).toBe(1)
    expect(parsed.columns[0].name).toBe('patient_id')
    expect(parsed.rows[0]).toEqual(['P1', 45])
  })

  test('parseSpreadsheetSheet handles .xls and .ods formats', async () => {
    const formats = ['xls', 'ods']
    
    for (const format of formats) {
      const filePath = path.join(uploadsDir, `test_spreadsheet.${format}`)
      const wb = XLSX.utils.book_new()
      const ws = XLSX.utils.aoa_to_sheet([['id', 'val'], [1, 'A']])
      XLSX.utils.book_append_sheet(wb, ws, 'Sheet1')
      XLSX.writeFile(wb, filePath)
      
      const parsed = await parseSpreadsheetSheet(filePath, 'Sheet1')
      expect(parsed.rowCount).toBe(1)
      expect(parsed.columns[0].name).toBe('id')
      expect(parsed.rows[0]).toEqual([1, 'A'])
      
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath)
    }
  })
})

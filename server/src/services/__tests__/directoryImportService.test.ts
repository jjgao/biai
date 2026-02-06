import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest'
import * as fs from 'fs/promises'
import * as path from 'path'
import * as os from 'os'

// Mock datasetService before importing the module under test
vi.mock('../datasetService.js', () => {
  const createDataset = vi.fn().mockResolvedValue({
    dataset_id: 'test-dataset-id',
    dataset_name: 'Test Dataset',
    database_name: 'ds_test',
    database_type: 'created',
    description: '',
    created_by: 'system',
    created_at: new Date(),
    updated_at: new Date(),
    tables: []
  })

  let tableCounter = 0
  const addTableToDataset = vi.fn().mockImplementation((_datasetId, tableName, displayName) => {
    tableCounter++
    return Promise.resolve({
      table_id: tableName,
      table_name: tableName,
      display_name: displayName,
      original_filename: 'test.txt',
      file_type: 'text/plain',
      row_count: 10,
      clickhouse_table_name: `ds_test.${tableName}`,
      schema_json: '[]',
      created_at: new Date()
    })
  })

  return {
    default: { createDataset, addTableToDataset },
    __resetCounter: () => { tableCounter = 0 }
  }
})

// Mock fileParser
vi.mock('../fileParser.js', () => ({
  parseCSVFile: vi.fn().mockResolvedValue({
    columns: [
      { name: 'id', type: 'String', nullable: false, index: 0 },
      { name: 'value', type: 'String', nullable: true, index: 1 }
    ],
    rows: [['1', 'a'], ['2', 'b']],
    rowCount: 2
  })
}))

import { importFromDirectory } from '../directoryImportService'
import datasetService from '../datasetService.js'
import { parseCSVFile } from '../fileParser.js'

describe('directoryImportService', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'dirimport-test-'))
    vi.clearAllMocks()
  })

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  test('should import a dataset from a directory with dataset.meta', async () => {
    // Create dataset.meta
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), `name: Test Dataset
description: A test dataset
tags: test,data
source: Unit Test`)

    // Create table meta and data file
    await fs.writeFile(path.join(tempDir, 'table1.meta'), `data_file: data1.txt
table_name: table_one
display_name: Table One
skip_rows: 0
delimiter: tab`)
    await fs.writeFile(path.join(tempDir, 'data1.txt'), 'id\tvalue\n1\ta\n2\tb')

    const result = await importFromDirectory(tempDir)

    expect(result.datasetId).toBe('test-dataset-id')
    expect(result.datasetName).toBe('Test Dataset')
    expect(result.tables).toHaveLength(1)
    expect(result.tables[0].tableName).toBe('table_one')
    expect(result.tables[0].displayName).toBe('Table One')

    // Verify createDataset was called with correct args
    expect(datasetService.createDataset).toHaveBeenCalledWith(
      'Test Dataset',
      'A test dataset',
      'system',
      ['test', 'data'],
      'Unit Test',
      '',
      [],
      expect.any(Object)
    )

    // Verify addTableToDataset was called
    expect(datasetService.addTableToDataset).toHaveBeenCalledTimes(1)
  })

  test('should fall back to directory name when no dataset.meta', async () => {
    // No dataset.meta, just a table
    await fs.writeFile(path.join(tempDir, 'table1.meta'), `data_file: data1.txt
table_name: my_table`)
    await fs.writeFile(path.join(tempDir, 'data1.txt'), 'id\tvalue\n1\ta')

    const result = await importFromDirectory(tempDir)

    expect(datasetService.createDataset).toHaveBeenCalledWith(
      path.basename(tempDir), // Falls back to directory name
      '',
      'system',
      [],
      '',
      '',
      [],
      {}
    )
    expect(result.tables).toHaveLength(1)
  })

  test('should skip tables with missing data files', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Test')
    await fs.writeFile(path.join(tempDir, 'good.meta'), `data_file: good.txt
table_name: good_table`)
    await fs.writeFile(path.join(tempDir, 'good.txt'), 'id\n1')
    await fs.writeFile(path.join(tempDir, 'bad.meta'), `data_file: missing.txt
table_name: bad_table`)

    const result = await importFromDirectory(tempDir)

    expect(result.tables).toHaveLength(1)
    expect(result.tables[0].tableName).toBe('good_table')
  })

  test('should skip tables without data_file field', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Test')
    await fs.writeFile(path.join(tempDir, 'no_data.meta'), `table_name: missing_ref`)
    await fs.writeFile(path.join(tempDir, 'good.meta'), `data_file: data.txt
table_name: good_table`)
    await fs.writeFile(path.join(tempDir, 'data.txt'), 'id\n1')

    const result = await importFromDirectory(tempDir)

    expect(result.tables).toHaveLength(1)
    expect(result.tables[0].tableName).toBe('good_table')
  })

  test('should return empty tables when no meta files exist', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Empty')

    const result = await importFromDirectory(tempDir)

    expect(result.tables).toHaveLength(0)
    expect(datasetService.addTableToDataset).not.toHaveBeenCalled()
  })

  test('should import tables in dependency order (topological sort)', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Test')

    // Parent table (no relationships)
    await fs.writeFile(path.join(tempDir, 'parent.meta'), `data_file: parent.txt
table_name: patients
display_name: Patients
primary_key: patient_id`)
    await fs.writeFile(path.join(tempDir, 'parent.txt'), 'patient_id\n1')

    // Child table (depends on parent)
    await fs.writeFile(path.join(tempDir, 'child.meta'), `data_file: child.txt
table_name: samples
display_name: Samples
relationship:
  foreign_key: patient_id
  references_table: patients
  references_column: patient_id
  type: many-to-one`)
    await fs.writeFile(path.join(tempDir, 'child.txt'), 'sample_id\tpatient_id\n1\t1')

    const result = await importFromDirectory(tempDir)

    expect(result.tables).toHaveLength(2)

    // Verify addTableToDataset call order: patients first, then samples
    const calls = (datasetService.addTableToDataset as any).mock.calls
    expect(calls[0][1]).toBe('patients')  // First call: parent table
    expect(calls[1][1]).toBe('samples')   // Second call: child table
  })

  test('should convert relationship format correctly', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Test')
    await fs.writeFile(path.join(tempDir, 'parent.meta'), `data_file: parent.txt
table_name: patients`)
    await fs.writeFile(path.join(tempDir, 'parent.txt'), 'id\n1')
    await fs.writeFile(path.join(tempDir, 'child.meta'), `data_file: child.txt
table_name: samples
relationship:
  foreign_key: patient_id
  references_table: patients
  references_column: patient_id
  type: many-to-one`)
    await fs.writeFile(path.join(tempDir, 'child.txt'), 'id\tpatient_id\n1\t1')

    await importFromDirectory(tempDir)

    // Find the call for the samples table
    const calls = (datasetService.addTableToDataset as any).mock.calls
    const samplesCall = calls.find((c: any[]) => c[1] === 'samples')
    expect(samplesCall).toBeDefined()

    // Check relationships arg (index 8)
    const relationships = samplesCall[8]
    expect(relationships).toHaveLength(1)
    expect(relationships[0]).toEqual({
      foreign_key: 'patient_id',
      referenced_table: 'patients',
      referenced_column: 'patient_id',
      type: 'many-to-one'
    })
  })

  test('should handle .meta file path (use parent directory)', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Test')
    await fs.writeFile(path.join(tempDir, 'table.meta'), `data_file: data.txt
table_name: my_table`)
    await fs.writeFile(path.join(tempDir, 'data.txt'), 'id\n1')

    // Pass path to a .meta file instead of directory
    const result = await importFromDirectory(path.join(tempDir, 'dataset.meta'))

    expect(result.tables).toHaveLength(1)
  })

  test('should pass column metadata config when specified', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Test')
    await fs.writeFile(path.join(tempDir, 'table.meta'), `data_file: data.txt
table_name: my_table
skip_rows: 4
column_display_name_row: 0
column_description_row: 1
column_datatype_row: 2
column_priority_row: 3`)
    await fs.writeFile(path.join(tempDir, 'data.txt'), 'id\n1')

    await importFromDirectory(tempDir)

    // Verify parseCSVFile was called with correct columnMetadataConfig
    expect(parseCSVFile).toHaveBeenCalledWith(
      expect.any(String),
      4,
      '\t',
      {
        displayNameRow: 0,
        descriptionRow: 1,
        dataTypeRow: 2,
        priorityRow: 3
      }
    )
  })

  test('should extract custom metadata from table meta', async () => {
    await fs.writeFile(path.join(tempDir, 'dataset.meta'), 'name: Test')
    await fs.writeFile(path.join(tempDir, 'table.meta'), `data_file: data.txt
table_name: my_table
genetic_alteration_type: CLINICAL
datatype: PATIENT_ATTRIBUTES`)
    await fs.writeFile(path.join(tempDir, 'data.txt'), 'id\n1')

    await importFromDirectory(tempDir)

    const calls = (datasetService.addTableToDataset as any).mock.calls
    // customMetadata is the 7th arg (index 7)
    const customMetadata = calls[0][7]
    expect(customMetadata.genetic_alteration_type).toBe('CLINICAL')
    expect(customMetadata.datatype).toBe('PATIENT_ATTRIBUTES')
  })

  test('should throw for non-existent path', async () => {
    await expect(importFromDirectory('/nonexistent/path')).rejects.toThrow()
  })
})

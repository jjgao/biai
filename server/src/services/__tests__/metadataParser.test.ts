import { describe, test, expect } from 'vitest'
import { parseMetadataFile, parseTableMetadata, parseDatasetMetadata } from '../metadataParser'

describe('metadataParser', () => {
  describe('parseMetadataFile', () => {
    test('should parse simple key-value pairs', () => {
      const content = `name: Test Dataset
description: A test
skip_rows: 4`
      const result = parseMetadataFile(content)
      expect(result.name).toBe('Test Dataset')
      expect(result.description).toBe('A test')
      expect(result.skip_rows).toBe(4)
    })

    test('should parse array values', () => {
      const content = `references:
  - pmid:123
  - pmid:456
  - doi:10.1234`
      const result = parseMetadataFile(content)
      expect(result.references).toEqual(['pmid:123', 'pmid:456', 'doi:10.1234'])
    })

    test('should parse nested object values', () => {
      const content = `relationship:
  foreign_key: patient_id
  references_table: patients
  references_column: patient_id
  type: many-to-one`
      const result = parseMetadataFile(content)
      expect(result.relationship).toEqual({
        foreign_key: 'patient_id',
        references_table: 'patients',
        references_column: 'patient_id',
        type: 'many-to-one'
      })
    })

    test('should skip comments and empty lines', () => {
      const content = `# This is a comment
name: Test

# Another comment
description: Hello`
      const result = parseMetadataFile(content)
      expect(result.name).toBe('Test')
      expect(result.description).toBe('Hello')
    })

    test('should parse boolean values', () => {
      const content = `enabled: true
disabled: false`
      const result = parseMetadataFile(content)
      expect(result.enabled).toBe(true)
      expect(result.disabled).toBe(false)
    })

    test('should parse comma-separated tags', () => {
      const content = `tags: cancer,genomics,tcga`
      const result = parseMetadataFile(content)
      expect(result.tags).toEqual(['cancer', 'genomics', 'tcga'])
    })

    test('should handle values with colons', () => {
      const content = `description: The data is here: http://example.com`
      const result = parseMetadataFile(content)
      expect(result.description).toBe('The data is here: http://example.com')
    })
  })

  describe('parseTableMetadata', () => {
    test('should parse basic table metadata', () => {
      const content = `data_file: data.txt
table_name: patients
display_name: Clinical Patients
skip_rows: 4
delimiter: tab
primary_key: patient_id`
      const result = parseTableMetadata(content)
      expect(result.data_file).toBe('data.txt')
      expect(result.table_name).toBe('patients')
      expect(result.display_name).toBe('Clinical Patients')
      expect(result.skip_rows).toBe(4)
      expect(result.delimiter).toBe('\t')
      expect(result.primary_key).toBe('patient_id')
    })

    test('should normalize comma delimiter', () => {
      const content = `delimiter: comma`
      const result = parseTableMetadata(content)
      expect(result.delimiter).toBe(',')
    })

    test('should parse singular relationship nested object format', () => {
      const content = `data_file: data.txt
table_name: samples
relationship:
  foreign_key: patient_id
  references_table: patients
  references_column: patient_id
  type: many-to-one`
      const result = parseTableMetadata(content)
      expect(result.relationships).toBeDefined()
      expect(result.relationships).toHaveLength(1)
      expect(result.relationships![0]).toEqual({
        foreign_key: 'patient_id',
        references: 'patients(patient_id)',
        type: 'many-to-one'
      })
    })

    test('should parse singular relationship without type (defaults to many-to-one)', () => {
      const content = `relationship:
  foreign_key: sample_id
  references_table: samples
  references_column: sample_id`
      const result = parseTableMetadata(content)
      expect(result.relationships).toHaveLength(1)
      expect(result.relationships![0].type).toBe('many-to-one')
    })

    test('should parse legacy foreign_key format', () => {
      const content = `foreign_key: (patient_id)
references: data_clinical_patient(patient_id)`
      const result = parseTableMetadata(content)
      expect(result.relationships).toBeDefined()
      expect(result.relationships).toHaveLength(1)
      expect(result.relationships![0].foreign_key).toBe('patient_id')
      expect(result.relationships![0].references).toBe('data_clinical_patient(patient_id)')
    })

    test('should preserve custom metadata fields', () => {
      const content = `data_file: data.txt
genetic_alteration_type: CLINICAL
datatype: PATIENT_ATTRIBUTES`
      const result = parseTableMetadata(content)
      expect(result.genetic_alteration_type).toBe('CLINICAL')
      expect(result.datatype).toBe('PATIENT_ATTRIBUTES')
    })
  })

  describe('parseDatasetMetadata', () => {
    test('should parse dataset metadata with all fields', () => {
      const content = `name: GBM Study
description: A study about GBM
tags: cancer,genomics
source: TCGA
citation: TCGA 2018
references:
  - pmid:123
  - doi:10.1234`
      const result = parseDatasetMetadata(content)
      expect(result.name).toBe('GBM Study')
      expect(result.description).toBe('A study about GBM')
      expect(result.tags).toEqual(['cancer', 'genomics'])
      expect(result.source).toBe('TCGA')
      expect(result.citation).toBe('TCGA 2018')
      expect(result.references).toEqual(['pmid:123', 'doi:10.1234'])
    })

    test('should preserve custom dataset fields', () => {
      const content = `name: Test
type_of_cancer: gbm
dataset_stable_id: test_123`
      const result = parseDatasetMetadata(content)
      expect(result.type_of_cancer).toBe('gbm')
      expect(result.dataset_stable_id).toBe('test_123')
    })
  })
})

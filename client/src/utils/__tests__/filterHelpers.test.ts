import { describe, expect, test } from 'vitest'
import {
  getFilterColumn,
  getFilterTableName,
  filterContainsColumn,
  findRelationshipPath,
  tablesHaveRelationship,
  getAllEffectiveFilters,
  unwrapNot,
  rangeKey,
  rangesEqual,
  getFilterCountKey,
  type Filter,
  type Table,
} from '../filterHelpers'

describe('filterHelpers', () => {
  describe('getFilterColumn', () => {
    test('returns column from simple filter', () => {
      const filter: Filter = {
        column: 'age',
        operator: 'eq',
        value: 25,
      }

      expect(getFilterColumn(filter)).toBe('age')
    })

    test('returns column from OR filter', () => {
      const filter: Filter = {
        or: [
          { column: 'status', operator: 'eq', value: 'Active' },
          { column: 'status', operator: 'eq', value: 'Pending' },
        ],
      }

      expect(getFilterColumn(filter)).toBe('status')
    })

    test('returns column from AND filter', () => {
      const filter: Filter = {
        and: [
          { column: 'age', operator: 'gte', value: 18 },
          { column: 'age', operator: 'lte', value: 65 },
        ],
      }

      expect(getFilterColumn(filter)).toBe('age')
    })

    test('returns column from NOT filter', () => {
      const filter: Filter = {
        not: {
          column: 'deleted',
          operator: 'eq',
          value: true,
        },
      }

      expect(getFilterColumn(filter)).toBe('deleted')
    })

    test('returns undefined for filter without column', () => {
      const filter: Filter = {
        or: [],
      }

      expect(getFilterColumn(filter)).toBeUndefined()
    })
  })

  describe('getFilterTableName', () => {
    test('returns tableName when present', () => {
      const filter: Filter = {
        column: 'age',
        operator: 'eq',
        value: 25,
        tableName: 'patients',
      }

      expect(getFilterTableName(filter)).toBe('patients')
    })

    test('returns undefined when tableName not present', () => {
      const filter: Filter = {
        column: 'age',
        operator: 'eq',
        value: 25,
      }

      expect(getFilterTableName(filter)).toBeUndefined()
    })
  })

  describe('filterContainsColumn', () => {
    test('returns true for direct column match', () => {
      const filter: Filter = {
        column: 'age',
        operator: 'eq',
        value: 25,
      }

      expect(filterContainsColumn(filter, 'age')).toBe(true)
    })

    test('returns false for non-matching column', () => {
      const filter: Filter = {
        column: 'age',
        operator: 'eq',
        value: 25,
      }

      expect(filterContainsColumn(filter, 'name')).toBe(false)
    })

    test('returns true for column in OR filter', () => {
      const filter: Filter = {
        or: [
          { column: 'status', operator: 'eq', value: 'Active' },
          { column: 'status', operator: 'eq', value: 'Pending' },
        ],
      }

      expect(filterContainsColumn(filter, 'status')).toBe(true)
    })

    test('returns true for column in AND filter', () => {
      const filter: Filter = {
        and: [
          { column: 'age', operator: 'gte', value: 18 },
          { column: 'name', operator: 'eq', value: 'John' },
        ],
      }

      expect(filterContainsColumn(filter, 'age')).toBe(true)
      expect(filterContainsColumn(filter, 'name')).toBe(true)
    })

    test('returns true for column in NOT filter', () => {
      const filter: Filter = {
        not: {
          column: 'deleted',
          operator: 'eq',
          value: true,
        },
      }

      expect(filterContainsColumn(filter, 'deleted')).toBe(true)
    })

    test('returns false for empty filter', () => {
      const filter: Filter = {}

      expect(filterContainsColumn(filter, 'age')).toBe(false)
    })
  })

  describe('findRelationshipPath', () => {
    const mockTables: Table[] = [
      {
        id: '1',
        name: 'patients',
        displayName: 'Patients',
        rowCount: 100,
        relationships: [],
      },
      {
        id: '2',
        name: 'samples',
        displayName: 'Samples',
        rowCount: 200,
        relationships: [
          {
            foreign_key: 'patient_id',
            referenced_table: 'patients',
            referenced_column: 'patient_id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: '3',
        name: 'mutations',
        displayName: 'Mutations',
        rowCount: 500,
        relationships: [
          {
            foreign_key: 'sample_id',
            referenced_table: 'samples',
            referenced_column: 'sample_id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: '4',
        name: 'unrelated',
        displayName: 'Unrelated',
        rowCount: 50,
        relationships: [],
      },
    ]

    test('finds direct relationship path', () => {
      const path = findRelationshipPath('samples', 'patients', mockTables)

      expect(path).toEqual(['samples', 'patients'])
    })

    test('finds reverse direct relationship path', () => {
      const path = findRelationshipPath('patients', 'samples', mockTables)

      expect(path).toEqual(['patients', 'samples'])
    })

    test('finds transitive 2-hop relationship path', () => {
      const path = findRelationshipPath('mutations', 'patients', mockTables)

      expect(path).toEqual(['mutations', 'samples', 'patients'])
    })

    test('finds reverse transitive 2-hop relationship path', () => {
      const path = findRelationshipPath('patients', 'mutations', mockTables)

      expect(path).toEqual(['patients', 'samples', 'mutations'])
    })

    test('returns null when no path exists', () => {
      const path = findRelationshipPath('patients', 'unrelated', mockTables)

      expect(path).toBeNull()
    })

    test('returns null for same table', () => {
      const path = findRelationshipPath('patients', 'patients', mockTables)

      expect(path).toBeNull()
    })
  })

  describe('tablesHaveRelationship', () => {
    const mockTables: Table[] = [
      {
        id: '1',
        name: 'patients',
        displayName: 'Patients',
        rowCount: 100,
        relationships: [],
      },
      {
        id: '2',
        name: 'samples',
        displayName: 'Samples',
        rowCount: 200,
        relationships: [
          {
            foreign_key: 'patient_id',
            referenced_table: 'patients',
            referenced_column: 'patient_id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: '3',
        name: 'mutations',
        displayName: 'Mutations',
        rowCount: 500,
        relationships: [
          {
            foreign_key: 'sample_id',
            referenced_table: 'samples',
            referenced_column: 'sample_id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: '4',
        name: 'unrelated',
        displayName: 'Unrelated',
        rowCount: 50,
        relationships: [],
      },
    ]

    test('returns true when table1 references table2', () => {
      const samples = mockTables[1]
      const patients = mockTables[0]

      expect(tablesHaveRelationship(samples, patients, mockTables)).toBe(true)
    })

    test('returns true when table2 references table1 (bidirectional)', () => {
      const patients = mockTables[0]
      const samples = mockTables[1]

      expect(tablesHaveRelationship(patients, samples, mockTables)).toBe(true)
    })

    test('returns true for transitive relationships', () => {
      const patients = mockTables[0]
      const mutations = mockTables[2]

      expect(tablesHaveRelationship(patients, mutations, mockTables)).toBe(true)
      expect(tablesHaveRelationship(mutations, patients, mockTables)).toBe(true)
    })

    test('returns false when no relationship exists', () => {
      const patients = mockTables[0]
      const unrelated = mockTables[3]

      expect(tablesHaveRelationship(patients, unrelated, mockTables)).toBe(false)
    })
  })

  describe('getAllEffectiveFilters', () => {
    const mockTables: Table[] = [
      {
        id: '1',
        name: 'patients',
        displayName: 'Patients',
        rowCount: 100,
        relationships: [],
      },
      {
        id: '2',
        name: 'samples',
        displayName: 'Samples',
        rowCount: 200,
        relationships: [
          {
            foreign_key: 'patient_id',
            referenced_table: 'patients',
            referenced_column: 'patient_id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: '3',
        name: 'mutations',
        displayName: 'Mutations',
        rowCount: 500,
        relationships: [
          {
            foreign_key: 'sample_id',
            referenced_table: 'samples',
            referenced_column: 'sample_id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: '4',
        name: 'unrelated',
        displayName: 'Unrelated',
        rowCount: 50,
        relationships: [],
      },
    ]

    test('categorizes direct filters correctly', () => {
      const filters: Filter[] = [
        {
          column: 'radiation_therapy',
          operator: 'eq',
          value: 'Yes',
          tableName: 'patients',
        },
      ]

      const result = getAllEffectiveFilters(filters, mockTables)

      expect(result.patients.direct).toHaveLength(1)
      expect(result.patients.direct[0]).toEqual(filters[0])
      expect(result.patients.propagated).toHaveLength(0)
    })

    test('categorizes propagated filters correctly', () => {
      const filters: Filter[] = [
        {
          column: 'radiation_therapy',
          operator: 'eq',
          value: 'Yes',
          tableName: 'patients',
        },
      ]

      const result = getAllEffectiveFilters(filters, mockTables)

      // The filter is direct for patients
      expect(result.patients.direct).toHaveLength(1)
      expect(result.patients.propagated).toHaveLength(0)

      // The filter is propagated to samples (has relationship with patients)
      expect(result.samples.direct).toHaveLength(0)
      expect(result.samples.propagated).toHaveLength(1)
      expect(result.samples.propagated[0]).toEqual(filters[0])

      // The filter is NOT propagated to unrelated (no relationship with patients)
      expect(result.unrelated.direct).toHaveLength(0)
      expect(result.unrelated.propagated).toHaveLength(0)
    })

    test('handles bidirectional relationships', () => {
      const filters: Filter[] = [
        {
          column: 'sample_type',
          operator: 'eq',
          value: 'Tumor',
          tableName: 'samples',
        },
      ]

      const result = getAllEffectiveFilters(filters, mockTables)

      // The filter is direct for samples
      expect(result.samples.direct).toHaveLength(1)
      expect(result.samples.propagated).toHaveLength(0)

      // The filter is propagated to patients (samples references patients)
      expect(result.patients.direct).toHaveLength(0)
      expect(result.patients.propagated).toHaveLength(1)
      expect(result.patients.propagated[0]).toEqual(filters[0])
    })

    test('handles multiple filters from different tables', () => {
      const filters: Filter[] = [
        {
          column: 'radiation_therapy',
          operator: 'eq',
          value: 'Yes',
          tableName: 'patients',
        },
        {
          column: 'sample_type',
          operator: 'eq',
          value: 'Tumor',
          tableName: 'samples',
        },
      ]

      const result = getAllEffectiveFilters(filters, mockTables)

      // Patients has 1 direct, 1 propagated (from samples)
      expect(result.patients.direct).toHaveLength(1)
      expect(result.patients.propagated).toHaveLength(1)

      // Samples has 1 direct, 1 propagated (from patients)
      expect(result.samples.direct).toHaveLength(1)
      expect(result.samples.propagated).toHaveLength(1)

      // Mutations has 0 direct, 2 propagated (from both patients and samples via transitive)
      expect(result.mutations.direct).toHaveLength(0)
      expect(result.mutations.propagated).toHaveLength(2)

      // Unrelated has 0 direct, 0 propagated (no relationships)
      expect(result.unrelated.direct).toHaveLength(0)
      expect(result.unrelated.propagated).toHaveLength(0)
    })

    test('handles transitive filter propagation', () => {
      const filters: Filter[] = [
        {
          column: 'age',
          operator: 'gte',
          value: 50,
          tableName: 'patients',
        },
      ]

      const result = getAllEffectiveFilters(filters, mockTables)

      // Patients has 1 direct
      expect(result.patients.direct).toHaveLength(1)
      expect(result.patients.propagated).toHaveLength(0)

      // Samples has 0 direct, 1 propagated (directly related to patients)
      expect(result.samples.direct).toHaveLength(0)
      expect(result.samples.propagated).toHaveLength(1)

      // Mutations has 0 direct, 1 propagated (transitively related via samples)
      expect(result.mutations.direct).toHaveLength(0)
      expect(result.mutations.propagated).toHaveLength(1)

      // Unrelated has 0 direct, 0 propagated
      expect(result.unrelated.direct).toHaveLength(0)
      expect(result.unrelated.propagated).toHaveLength(0)
    })

    test('ignores filters without tableName', () => {
      const filters: Filter[] = [
        {
          column: 'age',
          operator: 'eq',
          value: 25,
        },
      ]

      const result = getAllEffectiveFilters(filters, mockTables)

      expect(result.patients.direct).toHaveLength(0)
      expect(result.patients.propagated).toHaveLength(0)
      expect(result.samples.direct).toHaveLength(0)
      expect(result.samples.propagated).toHaveLength(0)
    })

    test('initializes all tables in result', () => {
      const filters: Filter[] = []

      const result = getAllEffectiveFilters(filters, mockTables)

      expect(result).toHaveProperty('patients')
      expect(result).toHaveProperty('samples')
      expect(result).toHaveProperty('mutations')
      expect(result).toHaveProperty('unrelated')
      expect(result.patients).toEqual({ direct: [], propagated: [] })
      expect(result.samples).toEqual({ direct: [], propagated: [] })
      expect(result.mutations).toEqual({ direct: [], propagated: [] })
      expect(result.unrelated).toEqual({ direct: [], propagated: [] })
    })
  })

  describe('unwrapNot', () => {
    test('returns undefined for undefined/null', () => {
      expect(unwrapNot(undefined)).toBeUndefined()
      expect(unwrapNot(null)).toBeUndefined()
    })

    test('returns filter itself if not NOT', () => {
      const filter: Filter = { column: 'a' }
      expect(unwrapNot(filter)).toBe(filter)
    })

    test('returns inner filter if NOT', () => {
      const inner: Filter = { column: 'a' }
      const filter: Filter = { not: inner }
      expect(unwrapNot(filter)).toBe(inner)
    })
  })

  describe('rangeKey', () => {
    test('generates expected key format', () => {
      expect(rangeKey('table1', 'col1', 'rows')).toBe('table1.col1:rows')
    })
    test('defaults to rows if countKey missing', () => {
      expect(rangeKey('table1', 'col1')).toBe('table1.col1:rows')
    })
  })

  describe('rangesEqual', () => {
    test('returns true for identical ranges', () => {
      expect(rangesEqual({ start: 1, end: 2 }, { start: 1, end: 2 })).toBe(true)
    })
    test('returns true for close enough ranges (epsilon)', () => {
      expect(rangesEqual({ start: 1, end: 2 }, { start: 1 + 1e-10, end: 2 - 1e-10 })).toBe(true)
    })
    test('returns false for different ranges', () => {
      expect(rangesEqual({ start: 1, end: 2 }, { start: 1.1, end: 2 })).toBe(false)
    })
  })

  describe('getFilterCountKey', () => {
    test('returns countByKey from filter', () => {
      const filter: Filter = { column: 'a', countByKey: 'parent:x' }
      expect(getFilterCountKey(filter)).toBe('parent:x')
    })
    test('returns rows default if missing', () => {
      const filter: Filter = { column: 'a' }
      expect(getFilterCountKey(filter)).toBe('rows') // strictly 'rows' based on default
    })
    test('returns countByKey from inner NOT filter', () => {
      const filter: Filter = {
        not: { column: 'a', countByKey: 'parent:y' }
      }
      expect(getFilterCountKey(filter)).toBe('parent:y')
    })
  })
})

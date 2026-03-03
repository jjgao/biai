import { describe, expect, test, beforeEach, vi } from 'vitest'

vi.mock('../../config/clickhouse.js', () => ({
  default: {
    query: vi.fn()
  }
}))

import aggregationService, { type Filter } from '../aggregationService'
import clickhouseClient from '../../config/clickhouse.js'

const mockQuery = vi.mocked(clickhouseClient.query)

const callPrivate = <T extends (...args: any[]) => any>(fnName: string) => {
  const fn = (aggregationService as unknown as Record<string, T>)[fnName]
  return fn.bind(aggregationService)
}

describe('AggregationService helpers', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  test('getCategoricalAggregation normalizes value and display_value', async () => {
    mockQuery.mockResolvedValueOnce({
      json: async () => [
        { value: '', display_value: '(Empty)', count: 10, percentage: 50 },
        { value: 'N/A', display_value: '(N/A)', count: 5, percentage: 25 },
        { value: 'yes', display_value: 'yes', count: 5, percentage: 25 }
      ]
    } as any)

    const result = await callPrivate('getCategoricalAggregation')(
      'test_table',
      'status',
      20,
      50,
      '',
      { type: 'rows' }
    )

    expect(mockQuery).toHaveBeenCalledTimes(1)
    expect(result.data).toEqual([
      { value: '', display_value: '(Empty)', count: 10, percentage: 50 },
      { value: 'N/A', display_value: '(N/A)', count: 5, percentage: 25 },
      { value: 'yes', display_value: 'yes', count: 5, percentage: 25 }
    ])
    expect(result.sql).toBeDefined()
  })

  test('buildFilterCondition handles empty, N/A, numeric values in IN filter', () => {
    const condition = callPrivate('buildFilterCondition')({
      column: 'status',
      operator: 'in',
      value: ['(Empty)', '(N/A)', 5]
    })

    expect(condition).toBe("(base_table.`status` IN ('N/A', 5) OR base_table.`status` = '' OR isNull(base_table.`status`))")
  })

  test('buildFilterCondition handles nulls in IN filter', () => {
    const condition = callPrivate('buildFilterCondition')({
      column: 'notes',
      operator: 'in',
      value: [null, 'value']
    })

    expect(condition).toBe("(base_table.`notes` IN ('value') OR isNull(base_table.`notes`))")
  })

  test('buildFilterCondition handles equality with (Empty)', () => {
    const condition = callPrivate('buildFilterCondition')({
      column: 'age_group',
      operator: 'eq',
      value: '(Empty)'
    })

    expect(condition).toBe("(base_table.`age_group` = '' OR isNull(base_table.`age_group`))")
  })

  test('buildFilterCondition handles equality with N/A literal', () => {
    const condition = callPrivate('buildFilterCondition')({
      column: 'age_group',
      operator: 'eq',
      value: '(N/A)'
    })

    expect(condition).toBe("base_table.`age_group` = 'N/A'")
  })
})

describe('AggregationService - Cross-Table Filtering', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  const mockTablesMetadata = [
    {
      table_name: 'patients',
      clickhouse_table_name: 'biai.patients_abc123',
      relationships: []
    },
    {
      table_name: 'samples',
      clickhouse_table_name: 'biai.samples_abc123',
      relationships: [
        {
          foreign_key: 'patient_id',
          referenced_table: 'patients',
          referenced_column: 'patient_id',
          type: 'many-to-one'
        }
      ]
    }
  ]

  const mockTransitiveTablesMetadata = [
    {
      table_name: 'patients',
      clickhouse_table_name: 'biai.patients_abc123',
      relationships: []
    },
    {
      table_name: 'samples',
      clickhouse_table_name: 'biai.samples_abc123',
      relationships: [
        {
          foreign_key: 'patient_id',
          referenced_table: 'patients',
          referenced_column: 'patient_id',
          type: 'many-to-one'
        }
      ]
    },
    {
      table_name: 'mutations',
      clickhouse_table_name: 'biai.mutations_abc123',
      relationships: [
        {
          foreign_key: 'sample_id',
          referenced_table: 'samples',
          referenced_column: 'sample_id',
          type: 'many-to-one'
        }
      ]
    }
  ]

  describe('buildCrossTableSubquery', () => {
    test('generates subquery for child→parent relationship (samples filtered by patients)', () => {
      const filter = {
        column: 'radiation_therapy',
        operator: 'eq' as const,
        value: 'Yes',
        tableName: 'patients'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'samples',
        filter,
        mockTablesMetadata
      )

      expect(subquery).toBe(
        "base_table.`patient_id` IN (SELECT `patient_id` FROM biai.patients_abc123 WHERE `radiation_therapy` = 'Yes')"
      )
    })

    test('generates subquery for parent→child relationship (patients filtered by samples)', () => {
      const filter = {
        column: 'sample_type',
        operator: 'eq' as const,
        value: 'Tumor',
        tableName: 'samples'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'patients',
        filter,
        mockTablesMetadata
      )

      expect(subquery).toBe(
        "base_table.`patient_id` IN (SELECT `patient_id` FROM biai.samples_abc123 WHERE `sample_type` = 'Tumor')"
      )
    })

    test('returns null when filter has no tableName', () => {
      const filter = {
        column: 'radiation_therapy',
        operator: 'eq' as const,
        value: 'Yes'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'patients',
        filter,
        mockTablesMetadata
      )

      expect(subquery).toBeNull()
    })

    test('returns null when filter tableName matches current table', () => {
      const filter = {
        column: 'radiation_therapy',
        operator: 'eq' as const,
        value: 'Yes',
        tableName: 'patients'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'patients',
        filter,
        mockTablesMetadata
      )

      expect(subquery).toBeNull()
    })

    test('returns null when no relationship exists between tables', () => {
      const metadataWithoutRels = [
        {
          table_name: 'patients',
          clickhouse_table_name: 'biai.patients_abc123',
          relationships: []
        },
        {
          table_name: 'treatments',
          clickhouse_table_name: 'biai.treatments_abc123',
          relationships: []
        }
      ]

      const filter = {
        column: 'treatment_type',
        operator: 'eq' as const,
        value: 'Chemotherapy',
        tableName: 'treatments'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'patients',
        filter,
        metadataWithoutRels
      )

      expect(subquery).toBeNull()
    })

    test('handles IN operator with multiple values', () => {
      const filter = {
        column: 'radiation_therapy',
        operator: 'in' as const,
        value: ['Yes', 'No'],
        tableName: 'patients'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'samples',
        filter,
        mockTablesMetadata
      )

      expect(subquery).toBe(
        "base_table.`patient_id` IN (SELECT `patient_id` FROM biai.patients_abc123 WHERE `radiation_therapy` IN ('Yes', 'No'))"
      )
    })

    test('handles BETWEEN operator for numeric ranges', () => {
      const filter = {
        column: 'age',
        operator: 'between' as const,
        value: [30, 50],
        tableName: 'patients'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'samples',
        filter,
        mockTablesMetadata
      )

      expect(subquery).toBe(
        'base_table.`patient_id` IN (SELECT `patient_id` FROM biai.patients_abc123 WHERE `age` BETWEEN 30 AND 50)'
      )
    })

    test('generates nested IN subquery for transitive 2-hop relationship (mutations→samples→patients)', () => {
      const filter = {
        column: 'age',
        operator: 'gte' as const,
        value: 50,
        tableName: 'patients'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'mutations',
        filter,
        mockTransitiveTablesMetadata
      )

      // Should use nested IN subqueries (ClickHouse-friendly, no JOINs)
      expect(subquery).toContain('`sample_id` IN')
      expect(subquery).toContain('SELECT `sample_id` FROM biai.samples_abc123')
      expect(subquery).toContain('`patient_id` IN')
      expect(subquery).toContain('SELECT `patient_id` FROM biai.patients_abc123')
      expect(subquery).toContain('`age` >= 50')
    })

    test('generates nested IN subquery for reverse transitive relationship (patients→samples→mutations)', () => {
      const filter = {
        column: 'gene',
        operator: 'eq' as const,
        value: 'TP53',
        tableName: 'mutations'
      }

      const subquery = callPrivate('buildCrossTableSubquery')(
        'patients',
        filter,
        mockTransitiveTablesMetadata
      )

      // Should use nested IN subqueries (ClickHouse-friendly, no JOINs)
      expect(subquery).toContain('`patient_id` IN')
      expect(subquery).toContain('SELECT `patient_id` FROM biai.samples_abc123')
      expect(subquery).toContain('`sample_id` IN')
      expect(subquery).toContain('SELECT `sample_id` FROM biai.mutations_abc123')
      expect(subquery).toContain("`gene` = 'TP53'")
    })
  })

  test('buildWhereClause uses alias resolver for ancestor filters', () => {
    const result = callPrivate('buildWhereClause')(
      [{ column: 'radiation_therapy', operator: 'eq', value: 'Yes', tableName: 'patients' }],
      undefined,
      'samples',
      mockTablesMetadata,
      (tableName?: string) => (tableName === 'patients' ? 'ancestor_patients' : undefined)
    )

    expect(result).toBe("AND (ancestor_patients.`radiation_therapy` = 'Yes')")
  })

  test('buildWhereClause handles NOT filters via alias resolver', () => {
    const result = callPrivate('buildWhereClause')(
      [{ not: { column: 'radiation_therapy', operator: 'eq', value: 'Yes', tableName: 'patients' } } as Filter],
      undefined,
      'samples',
      mockTablesMetadata,
      (tableName?: string) => (tableName === 'patients' ? 'ancestor_patients' : undefined)
    )

    expect(result).toBe("AND (NOT (ancestor_patients.`radiation_therapy` = 'Yes'))")
  })

  test('buildWhereClause generates NOT IN subquery for NOT wrapped cross-table filters', () => {
    const result = callPrivate('buildWhereClause')(
      [{ not: { column: 'radiation_therapy', operator: 'eq', value: 'Yes', tableName: 'patients' } } as Filter],
      undefined,
      'samples',
      mockTablesMetadata
    )

    expect(result).toContain('NOT IN')
    expect(result).toContain('SELECT `patient_id` FROM')
    expect(result).toContain("`radiation_therapy` = 'Yes'")
  })

  test('buildWhereClause handles NOT with OR combination on cross-table filters', () => {
    const result = callPrivate('buildWhereClause')(
      [
        {
          not: {
            or: [
              { column: 'radiation_therapy', operator: 'eq', value: 'Yes', tableName: 'patients' },
              { column: 'age', operator: 'gte', value: 60, tableName: 'patients' }
            ],
            tableName: 'patients'
          }
        } as Filter
      ],
      undefined,
      'samples',
      mockTablesMetadata
    )

    expect(result).toContain('NOT IN')
    expect(result).toContain('SELECT `patient_id` FROM')
    expect(result).toContain("`radiation_therapy` = 'Yes'")
    expect(result).toContain('`age` >= 60')
    expect(result).toContain(' OR ')
  })

  test('buildWhereClause handles multi-hop NOT IN cross-table filters', () => {
    const result = callPrivate('buildWhereClause')(
      [{ not: { column: 'age', operator: 'gte', value: 60, tableName: 'patients' } } as Filter],
      undefined,
      'mutations',
      mockTransitiveTablesMetadata
    )

    expect(result).toContain('NOT IN')
    // Should build nested subqueries: mutations.sample_id NOT IN (SELECT sample_id FROM samples WHERE patient_id IN (SELECT patient_id FROM patients WHERE age >= 60))
    expect(result).toContain('SELECT `sample_id` FROM')
    expect(result).toContain('SELECT `patient_id` FROM')
    expect(result).toContain('`age` >= 60')
  })

  test('buildWhereClause includes NULL guard for NOT IN cross-table filters', () => {
    const result = callPrivate('buildWhereClause')(
      [{ not: { column: 'radiation_therapy', operator: 'eq', value: 'Yes', tableName: 'patients' } } as Filter],
      undefined,
      'samples',
      mockTablesMetadata
    )

    // Should include NULL guard to preserve orphaned rows (samples with NULL patient_id)
    // Example: (base_table.patient_id NOT IN (...) OR base_table.patient_id IS NULL)
    expect(result).toContain('NOT IN')
    expect(result).toContain('IS NULL')
    expect(result).toContain('OR')
    expect(result).toContain("`radiation_therapy` = 'Yes'")
  })

  describe('findRelationshipPath', () => {
    test('finds direct relationship path', () => {
      const path = callPrivate('findRelationshipPath')(
        'samples',
        'patients',
        mockTablesMetadata
      )

      expect(path).toBeTruthy()
      expect(path).toHaveLength(1)
      expect(path[0].from).toBe('samples')
      expect(path[0].to).toBe('patients')
    })

    test('finds transitive 2-hop relationship path', () => {
      const path = callPrivate('findRelationshipPath')(
        'mutations',
        'patients',
        mockTransitiveTablesMetadata
      )

      expect(path).toBeTruthy()
      expect(path).toHaveLength(2)
      expect(path[0].from).toBe('mutations')
      expect(path[0].to).toBe('samples')
      expect(path[1].from).toBe('samples')
      expect(path[1].to).toBe('patients')
    })

    test('finds reverse transitive path', () => {
      const path = callPrivate('findRelationshipPath')(
        'patients',
        'mutations',
        mockTransitiveTablesMetadata
      )

      expect(path).toBeTruthy()
      expect(path).toHaveLength(2)
    })

    test('returns null when no path exists', () => {
      const noPathMetadata = [
        ...mockTransitiveTablesMetadata,
        {
          table_name: 'unrelated',
          clickhouse_table_name: 'biai.unrelated_abc123',
          relationships: []
        }
      ]

      const path = callPrivate('findRelationshipPath')(
        'patients',
        'unrelated',
        noPathMetadata
      )

      expect(path).toBeNull()
    })

    test('returns null for same table', () => {
      const path = callPrivate('findRelationshipPath')(
        'patients',
        'patients',
        mockTablesMetadata
      )

      expect(path).toBeNull()
    })
  })

  describe('buildWhereClause with cross-table filters', () => {
    test('combines local and cross-table filters with AND', () => {
      const filters = [
        {
          column: 'sample_type',
          operator: 'eq' as const,
          value: 'Tumor',
          tableName: 'samples'
        },
        {
          column: 'radiation_therapy',
          operator: 'eq' as const,
          value: 'Yes',
          tableName: 'patients'
        }
      ]

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['sample_type']), // samples table has sample_type column
        'samples',
        mockTablesMetadata
      )

      expect(whereClause).toContain("base_table.`sample_type` = 'Tumor'")
      expect(whereClause).toContain(
        "base_table.`patient_id` IN (SELECT `patient_id` FROM biai.patients_abc123 WHERE `radiation_therapy` = 'Yes')"
      )
      expect(whereClause).toContain('AND')
    })

    test('handles only cross-table filters', () => {
      const filters = [
        {
          column: 'radiation_therapy',
          operator: 'eq' as const,
          value: 'Yes',
          tableName: 'patients'
        }
      ]

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['sample_type', 'sample_id']), // samples columns
        'samples',
        mockTablesMetadata
      )

      expect(whereClause).toBe(
        "AND (base_table.`patient_id` IN (SELECT `patient_id` FROM biai.patients_abc123 WHERE `radiation_therapy` = 'Yes'))"
      )
    })

    test('handles only local filters', () => {
      const filters = [
        {
          column: 'sample_type',
          operator: 'eq' as const,
          value: 'Tumor',
          tableName: 'samples'
        }
      ]

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['sample_type']),
        'samples',
        mockTablesMetadata
      )

      expect(whereClause).toBe("AND (base_table.`sample_type` = 'Tumor')")
    })

    test('filters out non-existent columns from local filters', () => {
      const filters = [
        {
          column: 'non_existent_column',
          operator: 'eq' as const,
          value: 'value',
          tableName: 'samples'
        },
        {
          column: 'sample_type',
          operator: 'eq' as const,
          value: 'Tumor',
          tableName: 'samples'
        }
      ]

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['sample_type']), // only sample_type exists
        'samples',
        mockTablesMetadata
      )

      // Should only include sample_type, not non_existent_column
      expect(whereClause).toBe("AND (base_table.`sample_type` = 'Tumor')")
      expect(whereClause).not.toContain('non_existent_column')
    })

    test('returns empty string when no valid filters', () => {
      const filters = [
        {
          column: 'non_existent_column',
          operator: 'eq' as const,
          value: 'value',
          tableName: 'samples'
        }
      ]

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['sample_type']), // column doesn't exist
        'samples',
        mockTablesMetadata
      )

      expect(whereClause).toBe('')
    })

    test('handles multiple cross-table filters from different tables', () => {
      const extendedMetadata = [
        ...mockTablesMetadata,
        {
          table_name: 'treatments',
          clickhouse_table_name: 'biai.treatments_abc123',
          relationships: [
            {
              foreign_key: 'patient_id',
              referenced_table: 'patients',
              referenced_column: 'patient_id',
              type: 'many-to-one'
            }
          ]
        }
      ]

      const filters = [
        {
          column: 'sample_type',
          operator: 'eq' as const,
          value: 'Tumor',
          tableName: 'samples'
        },
        {
          column: 'treatment_type',
          operator: 'eq' as const,
          value: 'Chemotherapy',
          tableName: 'treatments'
        }
      ]

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['patient_id', 'radiation_therapy']),
        'patients',
        extendedMetadata
      )

      expect(whereClause).toContain(
        "base_table.`patient_id` IN (SELECT `patient_id` FROM biai.samples_abc123 WHERE `sample_type` = 'Tumor')"
      )
      expect(whereClause).toContain(
        "base_table.`patient_id` IN (SELECT `patient_id` FROM biai.treatments_abc123 WHERE `treatment_type` = 'Chemotherapy')"
      )
    })
  })

  describe('buildWhereClause with logical operators and cross-table filters', () => {
    test('handles OR filters with cross-table filtering', () => {
      const filters = {
        or: [
          {
            column: 'sample_type',
            operator: 'eq' as const,
            value: 'Tumor',
            tableName: 'samples'
          },
          {
            column: 'sample_type',
            operator: 'eq' as const,
            value: 'Normal',
            tableName: 'samples'
          }
        ]
      }

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['sample_type']),
        'samples',
        mockTablesMetadata
      )

      expect(whereClause).toContain("base_table.`sample_type` = 'Tumor'")
      expect(whereClause).toContain("base_table.`sample_type` = 'Normal'")
      expect(whereClause).toContain('OR')
    })

    test('handles AND filters with cross-table filtering', () => {
      // When filters are combined with AND, they're passed as an array (not wrapped in an 'and' object)
      const filters = [
        {
          column: 'sample_type',
          operator: 'eq' as const,
          value: 'Tumor',
          tableName: 'samples'
        },
        {
          column: 'radiation_therapy',
          operator: 'eq' as const,
          value: 'Yes',
          tableName: 'patients'
        }
      ]

      const whereClause = callPrivate('buildWhereClause')(
        filters,
        new Set(['sample_type']),
        'samples',
        mockTablesMetadata
      )

      expect(whereClause).toContain("base_table.`sample_type` = 'Tumor'")
      expect(whereClause).toContain(
        "base_table.`patient_id` IN (SELECT `patient_id` FROM biai.patients_abc123 WHERE `radiation_therapy` = 'Yes')"
      )
    })
  })
})

describe('AggregationService - countBy metrics', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  const metadata: any = [
    {
      table_name: 'samples',
      clickhouse_table_name: 'biai.samples_raw',
      relationships: [
        {
          foreign_key: 'patient_id',
          referenced_table: 'patients',
          referenced_column: 'patient_id',
          type: 'many-to-one'
        }
      ]
    },
    {
      table_name: 'patients',
      clickhouse_table_name: 'biai.patients_raw',
      relationships: []
    }
  ]

  test('resolveMetricContext returns parent configuration', () => {
    const context = callPrivate('resolveMetricContext')(
      'samples',
      { mode: 'parent', target_table: 'patients' },
      metadata
    )

    expect(context).toEqual({
      type: 'parent',
      parentTable: 'patients',
      parentColumn: 'patient_id',
      joins: [
        {
          alias: 'ancestor_0',
          table: 'biai.patients_raw',
          on: 'base_table.`patient_id` = ancestor_0.`patient_id`'
        }
      ],
      ancestorExpression: 'ancestor_0.`patient_id`',
      pathSegments: [
        { from_table: 'samples', via_column: 'patient_id', to_table: 'patients', referenced_column: 'patient_id' }
      ],
      aliasByTable: expect.objectContaining({
        samples: 'base_table',
        patients: 'ancestor_0'
      }),
      parentAlias: 'ancestor_0'
    })
  })

  test('resolveMetricContext throws descriptive 400 error when relationship missing', () => {
    expect.assertions(2)

    try {
      callPrivate('resolveMetricContext')(
        'samples',
        { mode: 'parent', target_table: 'unknown' },
        metadata
      )
    } catch (error: any) {
      expect(error.status).toBe(400)
      expect(error.message).toContain('No relationship')
    }
  })

  test('getColumnAggregation returns parent metric metadata', async () => {
    const getTableColumnsSpy = vi.spyOn(aggregationService as any, 'getTableColumns')
      .mockResolvedValue(new Set(['status']))

    mockQuery
      // 1. Table info query
      .mockResolvedValueOnce({
        json: async () => [{ table_name: 'samples', clickhouse_table_name: 'biai.samples_raw', row_count: 10 }]
      } as any)
      // 2. List columns query (when listColumns not provided)
      .mockResolvedValueOnce({
        json: async () => []
      } as any)
      // 3. Filtered count query (for parent metric)
      .mockResolvedValueOnce({
        json: async () => [{ filtered_count: 3 }]
      } as any)
      // 4. Basic stats query
      .mockResolvedValueOnce({
        json: async () => [{ null_count: 1, unique_count: 2 }]
      } as any)
      // 5. Categorical aggregation query
      .mockResolvedValueOnce({
        json: async () => [{ value: 'A', display_value: 'A', count: 2, percentage: 100 }]
      } as any)

    const result = await aggregationService.getColumnAggregation(
      'dataset-1',
      'table-1',
      'status',
      'categorical',
      [],
      'samples',
      metadata,
      { mode: 'parent', target_table: 'patients' }
    )

    expect(result.metric_type).toBe('parent')
    expect(result.metric_parent_table).toBe('patients')
    expect(result.total_rows).toBe(3)
    expect(result.categories).toEqual([{ value: 'A', display_value: 'A', count: 2, percentage: 100 }])
    expect(result.metric_path).toEqual([
      { from_table: 'samples', via_column: 'patient_id', to_table: 'patients', referenced_column: 'patient_id' }
    ])
    expect(mockQuery).toHaveBeenCalledTimes(5)

    getTableColumnsSpy.mockRestore()
  })

  test('resolveMetricContext builds joins for multi-hop ancestor', () => {
    const extendedMetadata = [
      {
        table_name: 'mutations',
        clickhouse_table_name: 'biai.mutations_raw',
        relationships: [
          { foreign_key: 'sample_id', referenced_table: 'samples', referenced_column: 'sample_id', type: 'many-to-one' }
        ]
      },
      {
        table_name: 'samples',
        clickhouse_table_name: 'biai.samples_raw',
        relationships: [
          { foreign_key: 'patient_id', referenced_table: 'patients', referenced_column: 'patient_id', type: 'many-to-one' }
        ]
      },
      {
        table_name: 'patients',
        clickhouse_table_name: 'biai.patients_raw',
        relationships: [
          { foreign_key: 'hospital_id', referenced_table: 'hospitals', referenced_column: 'hospital_id', type: 'many-to-one' }
        ]
      },
      {
        table_name: 'hospitals',
        clickhouse_table_name: 'biai.hospitals_raw',
        relationships: []
      }
    ]

    const context = callPrivate('resolveMetricContext')(
      'mutations',
      { mode: 'parent', target_table: 'hospitals' },
      extendedMetadata
    )

    expect(context.joins).toEqual([
      {
        alias: 'ancestor_0',
        table: 'biai.samples_raw',
        on: 'base_table.`sample_id` = ancestor_0.`sample_id`'
      },
      {
        alias: 'ancestor_1',
        table: 'biai.patients_raw',
        on: 'ancestor_0.`patient_id` = ancestor_1.`patient_id`'
      },
      {
        alias: 'ancestor_2',
        table: 'biai.hospitals_raw',
        on: 'ancestor_1.`hospital_id` = ancestor_2.`hospital_id`'
      }
    ])
    expect(context.ancestorExpression).toBe('ancestor_2.`hospital_id`')
    expect(context.parentTable).toBe('hospitals')
    expect(context.parentColumn).toBe('hospital_id')
    expect(context.pathSegments).toEqual([
      { from_table: 'mutations', via_column: 'sample_id', to_table: 'samples', referenced_column: 'sample_id' },
      { from_table: 'samples', via_column: 'patient_id', to_table: 'patients', referenced_column: 'patient_id' },
      { from_table: 'patients', via_column: 'hospital_id', to_table: 'hospitals', referenced_column: 'hospital_id' }
    ])
    expect(context.aliasByTable).toEqual({
      mutations: 'base_table',
      samples: 'ancestor_0',
      patients: 'ancestor_1',
      hospitals: 'ancestor_2'
    })
    expect(context.parentAlias).toBe('ancestor_2')
  })

  test('getTableAggregations returns multi-hop parent metrics end-to-end', async () => {
    const getTableColumnsSpy = vi.spyOn(aggregationService as any, 'getTableColumns')
      .mockResolvedValue(new Set(['status']))

    const datasetTables = [
      { table_id: 'table-mutations', table_name: 'mutations', clickhouse_table_name: 'biai.mutations_raw' },
      { table_id: 'table-samples', table_name: 'samples', clickhouse_table_name: 'biai.samples_raw' },
      { table_id: 'table-patients', table_name: 'patients', clickhouse_table_name: 'biai.patients_raw' },
      { table_id: 'table-hospitals', table_name: 'hospitals', clickhouse_table_name: 'biai.hospitals_raw' }
    ]

    const relationships = [
      {
        table_id: 'table-mutations',
        foreign_key: 'sample_id',
        referenced_table: 'samples',
        referenced_column: 'sample_id',
        relationship_type: 'many-to-one'
      },
      {
        table_id: 'table-samples',
        foreign_key: 'patient_id',
        referenced_table: 'patients',
        referenced_column: 'patient_id',
        relationship_type: 'many-to-one'
      },
      {
        table_id: 'table-patients',
        foreign_key: 'hospital_id',
        referenced_table: 'hospitals',
        referenced_column: 'hospital_id',
        relationship_type: 'many-to-one'
      }
    ]

    mockQuery
      // 1. loadDatasetTablesMetadata - tables
      .mockResolvedValueOnce({
        json: async () => datasetTables
      } as any)
      // 2. loadDatasetTablesMetadata - relationships
      .mockResolvedValueOnce({
        json: async () => relationships
      } as any)
      // 3. dataset_columns (column metadata query in getTableAggregations)
      .mockResolvedValueOnce({
        json: async () => [
          { column_name: 'status', display_type: 'categorical', is_hidden: false }
        ]
      } as any)
      // 4. list columns query in getTableAggregations
      .mockResolvedValueOnce({
        json: async () => []
      } as any)
      // 5. dataset_tables lookup in getColumnAggregation
      .mockResolvedValueOnce({
        json: async () => [{ table_name: 'mutations', clickhouse_table_name: 'biai.mutations_raw', row_count: 100 }]
      } as any)
      // 6. count query (for parent metric)
      .mockResolvedValueOnce({
        json: async () => [{ filtered_count: 80 }]
      } as any)
      // 7. basic stats
      .mockResolvedValueOnce({
        json: async () => [{ null_count: 5, unique_count: 10 }]
      } as any)
      // 8. categorical aggregation
      .mockResolvedValueOnce({
        json: async () => [{ value: 'A', display_value: 'A', count: 80, percentage: 100 }]
      } as any)

    const aggregations = await aggregationService.getTableAggregations(
      'dataset-1',
      'table-mutations',
      [],
      { mode: 'parent', target_table: 'hospitals' }
    )

    expect(aggregations).toHaveLength(1)
    const [aggregation] = aggregations
    expect(aggregation.metric_type).toBe('parent')
    expect(aggregation.metric_parent_table).toBe('hospitals')
    expect(aggregation.metric_path).toEqual([
      { from_table: 'mutations', via_column: 'sample_id', to_table: 'samples', referenced_column: 'sample_id' },
      { from_table: 'samples', via_column: 'patient_id', to_table: 'patients', referenced_column: 'patient_id' },
      { from_table: 'patients', via_column: 'hospital_id', to_table: 'hospitals', referenced_column: 'hospital_id' }
    ])
    expect(aggregation.total_rows).toBe(80)
    expect(mockQuery).toHaveBeenCalledTimes(8)

    getTableColumnsSpy.mockRestore()
  })

  test('NOT filter with parent counting uses parent-level exclusion semantics', () => {
    const metricContext = {
      type: 'parent' as const,
      joins: [
        {
          alias: 'ancestor_0',
          table: 'biai.patients_raw',
          on: 'base_table.patient_id = ancestor_0.patient_id'
        }
      ],
      pathSegments: [
        {
          from_table: 'samples',
          via_column: 'patient_id',
          to_table: 'patients',
          referenced_column: 'patient_id'
        }
      ]
    }

    const filter = {
      not: {
        column: 'sample_type',
        operator: 'eq' as const,
        value: 'Primary',
        tableName: 'samples'
      }
    }

    const result = callPrivate('buildWhereClause')(
      [filter],
      undefined,
      'samples',
      metadata,
      undefined,
      metricContext,
      'biai.samples_raw'
    )

    // Should use parent-level exclusion: parent_id NOT IN (SELECT patient_id FROM samples WHERE sample_type = 'Primary')
    expect(result).toContain('NOT IN')
    expect(result).toContain('SELECT `patient_id`')
    expect(result).toContain('FROM biai.samples_raw')
    expect(result).toContain("`sample_type` = 'Primary'")
    expect(result).toContain('base_table.`patient_id`')
  })

  test('NOT filter with parent counting excludes parents with ANY matching child', () => {
    const metricContext = {
      type: 'parent' as const,
      joins: [
        {
          alias: 'ancestor_0',
          table: 'biai.patients_raw',
          on: 'base_table.patient_id = ancestor_0.patient_id'
        }
      ],
      pathSegments: [
        {
          from_table: 'samples',
          via_column: 'patient_id',
          to_table: 'patients',
          referenced_column: 'patient_id'
        }
      ]
    }

    const filter = {
      not: {
        column: 'sample_type',
        operator: 'in' as const,
        value: ['Primary', 'Recurrent'],
        tableName: 'samples'
      }
    }

    const result = callPrivate('buildWhereClause')(
      [filter],
      undefined,
      'samples',
      metadata,
      undefined,
      metricContext,
      'biai.samples_raw'
    )

    // Should exclude parents where ANY child has sample_type IN ('Primary', 'Recurrent')
    expect(result).toContain('NOT IN')
    expect(result).toContain('SELECT `patient_id`')
    expect(result).toContain("`sample_type` IN ('Primary'")
    expect(result).toContain("'Recurrent')")
  })

  test('NOT filter without parent counting uses row-level semantics', () => {
    const metricContext = {
      type: 'rows' as const
    }

    const filter = {
      not: {
        column: 'sample_type',
        operator: 'eq' as const,
        value: 'Primary'
      }
    }

    const result = callPrivate('buildWhereClause')(
      [filter],
      undefined,
      'samples',
      metadata,
      undefined,
      metricContext,
      'biai.samples_raw'
    )

    // Should use row-level NOT (no subquery)
    expect(result).toContain('NOT')
    expect(result).toContain("`sample_type` = 'Primary'")
    expect(result).not.toContain('NOT IN')
    expect(result).not.toContain('SELECT')
  })

  test('NOT filter on parent attribute with parent counting uses regular NOT', () => {
    const metricContext = {
      type: 'parent' as const,
      joins: [
        {
          alias: 'ancestor_0',
          table: 'biai.patients_raw',
          foreignKey: 'patient_id',
          on: 'base_table.patient_id = ancestor_0.patient_id'
        }
      ],
      aliasByTable: {
        samples: 'base_table',
        patients: 'ancestor_0'
      }
    }

    const aliasResolver = (tableName?: string) => {
      if (tableName === 'patients') return 'ancestor_0'
      if (tableName === 'samples') return 'base_table'
      return undefined
    }

    const filter = {
      not: {
        column: 'age',
        operator: 'gte' as const,
        value: 60,
        tableName: 'patients'
      }
    }

    const result = callPrivate('buildWhereClause')(
      [filter],
      undefined,
      'samples',
      metadata,
      aliasResolver,
      metricContext,
      'biai.samples_raw'
    )

    // Parent-level filters should use regular NOT with alias (not exclusion subquery)
    expect(result).toContain('NOT')
    expect(result).toContain('ancestor_0.`age`')
    expect(result).not.toContain('NOT IN')
  })

  test('NOT filter with multi-hop parent counting joins through full relationship path', () => {
    const multiHopMetadata: any = [
      {
        table_name: 'mutations',
        clickhouse_table_name: 'biai.mutations_raw',
        relationships: [
          { foreign_key: 'sample_id', referenced_table: 'samples', referenced_column: 'sample_id', type: 'many-to-one' }
        ]
      },
      {
        table_name: 'samples',
        clickhouse_table_name: 'biai.samples_raw',
        relationships: [
          { foreign_key: 'patient_id', referenced_table: 'patients', referenced_column: 'patient_id', type: 'many-to-one' }
        ]
      },
      {
        table_name: 'patients',
        clickhouse_table_name: 'biai.patients_raw',
        relationships: []
      }
    ]

    // Simulate metricContext for counting mutations by patients (2-hop path)
    const metricContext = {
      type: 'parent' as const,
      parentTable: 'patients',
      parentColumn: 'patient_id',
      parentAlias: 'ancestor_1',
      joins: [
        {
          alias: 'ancestor_0',
          table: 'biai.samples_raw',
          on: 'base_table.`sample_id` = ancestor_0.`sample_id`'
        },
        {
          alias: 'ancestor_1',
          table: 'biai.patients_raw',
          on: 'ancestor_0.`patient_id` = ancestor_1.`patient_id`'
        }
      ],
      pathSegments: [
        { from_table: 'mutations', via_column: 'sample_id', to_table: 'samples', referenced_column: 'sample_id' },
        { from_table: 'samples', via_column: 'patient_id', to_table: 'patients', referenced_column: 'patient_id' }
      ],
      aliasByTable: {
        mutations: 'base_table',
        samples: 'ancestor_0',
        patients: 'ancestor_1'
      }
    }

    const filter = {
      not: {
        column: 'hugo_symbol',
        operator: 'eq' as const,
        value: 'EGFR',
        tableName: 'mutations'
      }
    }

    const result = callPrivate('buildWhereClause')(
      [filter],
      undefined,
      'mutations',
      multiHopMetadata,
      undefined,
      metricContext,
      'biai.mutations_raw'
    )

    // Multi-hop exclusion: the outer reference must be ancestor_0.`patient_id`
    // (patient_id lives in samples, which is the ancestor_0 join)
    expect(result).toContain('ancestor_0.`patient_id`')

    // The subquery must join mutations to samples to collect patient_ids
    expect(result).toContain('NOT IN')
    expect(result).toContain('FROM biai.mutations_raw')
    expect(result).toContain('ANY LEFT JOIN biai.samples_raw AS ancestor_0')
    expect(result).toContain('ancestor_0.`sample_id`')

    // The WHERE condition inside the subquery should NOT have an alias prefix
    expect(result).toContain("`hugo_symbol` = 'EGFR'")
    expect(result).not.toContain('base_table.`hugo_symbol`')

    // The subquery must NOT join all the way to patients (patient_id is already in samples)
    expect(result).not.toContain('biai.patients_raw')
  })
})

describe('AggregationService - Bivariate Aggregation', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  const mockBivariateTableData = {
    table_name: 'samples',
    clickhouse_table_name: 'biai.samples_abc123',
    row_count: 100
  }

  test('getBivariateAggregation throws error for missing table', async () => {
    // Mock table lookup to return empty
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any)

    await expect(
      aggregationService.getBivariateAggregation('test_dataset', 'unknown_table', 'col1', 'col2')
    ).rejects.toThrow('Table not found')
  })

  test('getBivariateAggregation validates column existence', async () => {
    // Mock table lookup  
    mockQuery.mockResolvedValueOnce({ json: async () => [mockBivariateTableData] } as any)
    // Mock list columns (empty)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any)
    // Mock table metadata loading
    mockQuery.mockResolvedValueOnce({ json: async () => [mockBivariateTableData] } as any)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // relationships
    // Mock valid columns
    mockQuery.mockResolvedValueOnce({
      json: async () => [{ name: 'col1' }, { name: 'other_col' }]
    } as any)

    await expect(
      aggregationService.getBivariateAggregation('test_dataset', 'test_table', 'col1', 'missing_col')
    ).rejects.toThrow("Column 'missing_col' not found in table")
  })

  test('getBivariateAggregation validates both columns exist', async () => {
    // Mock table lookup  
    mockQuery.mockResolvedValueOnce({ json: async () => [mockBivariateTableData] } as any)
    // Mock list columns (empty)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any)
    // Mock table metadata loading
    mockQuery.mockResolvedValueOnce({ json: async () => [mockBivariateTableData] } as any)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // relationships
    // Mock valid columns with missing x column
    mockQuery.mockResolvedValueOnce({
      json: async () => [{ name: 'col2' }, { name: 'other_col' }]
    } as any)

    await expect(
      aggregationService.getBivariateAggregation('test_dataset', 'test_table', 'missing_x', 'col2')
    ).rejects.toThrow("Column 'missing_x' not found in table")
  })

  test('getBivariateAggregation returns expected structure with top-10 bucketing', async () => {
    const mockTableData = [mockBivariateTableData]
    
    // Mock table lookup
    mockQuery.mockResolvedValueOnce({ json: async () => mockTableData } as any)
    // Mock list columns (empty)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any)
    // Mock table metadata loading
    mockQuery.mockResolvedValueOnce({ json: async () => mockTableData } as any)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // relationships
    // Mock valid columns
    mockQuery.mockResolvedValueOnce({
      json: async () => [{ name: 'status' }, { name: 'type' }]
    } as any)
    // Mock total count
    mockQuery.mockResolvedValueOnce({ json: async () => [{ total_count: 100 }] } as any)
    // Mock top X categories
    mockQuery.mockResolvedValueOnce({
      json: async () => [
        { val: 'Active', cnt: 60 },
        { val: 'Inactive', cnt: 25 },
        { val: 'Pending', cnt: 15 }
      ]
    } as any)
    // Mock top Y categories
    mockQuery.mockResolvedValueOnce({
      json: async () => [
        { val: 'Primary', cnt: 70 },
        { val: 'Secondary', cnt: 30 }
      ]
    } as any)
    // Mock cross-tabulation data
    mockQuery.mockResolvedValueOnce({
      json: async () => [
        { x: 'Active', y: 'Primary', count: 50 },
        { x: 'Active', y: 'Secondary', count: 10 },
        { x: 'Inactive', y: 'Primary', count: 20 },
        { x: 'Inactive', y: 'Secondary', count: 5 },
        { x: 'Pending', y: 'Primary', count: 0 },
        { x: 'Pending', y: 'Secondary', count: 15 }
      ]
    } as any)

    const result = await aggregationService.getBivariateAggregation(
      'test_dataset', 'test_table', 'status', 'type'
    )

    expect(result).toEqual({
      x_column: 'status',
      y_column: 'type',
      data: [
        { x: 'Active', y: 'Primary', count: 50 },
        { x: 'Active', y: 'Secondary', count: 10 },
        { x: 'Inactive', y: 'Primary', count: 20 },
        { x: 'Inactive', y: 'Secondary', count: 5 },
        { x: 'Pending', y: 'Primary', count: 0 },
        { x: 'Pending', y: 'Secondary', count: 15 }
      ],
      x_categories: ['Active', 'Inactive', 'Pending'],
      y_categories: ['Primary', 'Secondary'],
      total_rows: 100,
      metric_type: 'rows',
      sql: expect.any(String)
    })
  })

  test('getBivariateAggregation handles "Other" bucketing for high cardinality', async () => {
    const mockTableData = [mockBivariateTableData]
    
    // Mock the setup calls 
    mockQuery.mockResolvedValueOnce({ json: async () => mockTableData } as any)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // list columns
    mockQuery.mockResolvedValueOnce({ json: async () => mockTableData } as any) // metadata
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // relationships
    mockQuery.mockResolvedValueOnce({
      json: async () => [{ name: 'category' }, { name: 'subcategory' }]
    } as any) // valid columns
    mockQuery.mockResolvedValueOnce({ json: async () => [{ total_count: 1000 }] } as any) // total count

    // Mock top 10 X categories (simulating high cardinality)
    const topXCategories = Array.from({ length: 10 }, (_, i) => ({ 
      val: `Category${i + 1}`, 
      cnt: 100 - i * 5 
    }))
    mockQuery.mockResolvedValueOnce({ json: async () => topXCategories } as any)

    // Mock top 10 Y categories  
    const topYCategories = Array.from({ length: 10 }, (_, i) => ({ 
      val: `SubCategory${i + 1}`, 
      cnt: 90 - i * 4 
    }))
    mockQuery.mockResolvedValueOnce({ json: async () => topYCategories } as any)

    // Mock cross-tabulation with some "Other" categories
    mockQuery.mockResolvedValueOnce({
      json: async () => [
        { x: 'Category1', y: 'SubCategory1', count: 50 },
        { x: 'Other', y: 'SubCategory1', count: 20 },
        { x: 'Category1', y: 'Other', count: 15 }
      ]
    } as any)

    const result = await aggregationService.getBivariateAggregation(
      'test_dataset', 'test_table', 'category', 'subcategory'
    )

    expect(result.x_categories).toHaveLength(11) // 10 top categories + "Other"
    expect(result.y_categories).toHaveLength(11) // 10 top subcategories + "Other"
    expect(result.data).toContainEqual({ x: 'Other', y: 'SubCategory1', count: 20 })
    expect(result.x_categories).toContain('Category1')
    expect(result.y_categories).toContain('SubCategory1')
    expect(result.total_rows).toBe(1000)
  })

  test('getBivariateAggregation works with parent counting mode', async () => {
    const mockTableData = [{ 
      table_name: 'samples',
      clickhouse_table_name: 'biai.samples_abc123',
      row_count: 100
    }]
    
    const mockMetadata = [
      {
        table_name: 'patients',
        clickhouse_table_name: 'biai.patients_abc123',
        relationships: []
      },
      {
        table_name: 'samples',
        clickhouse_table_name: 'biai.samples_abc123', 
        relationships: [
          {
            foreign_key: 'patient_id',
            referenced_table: 'patients',
            referenced_column: 'patient_id',
            type: 'many-to-one'
          }
        ]
      }
    ]

    // Setup mocks
    mockQuery.mockResolvedValueOnce({ json: async () => mockTableData } as any) // table lookup
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // list columns
    
    // Table metadata loading
    mockQuery.mockResolvedValueOnce({ 
      json: async () => [
        { table_id: 'test_table', table_name: 'samples', clickhouse_table_name: 'biai.samples_abc123' },
        { table_id: 'patients_table', table_name: 'patients', clickhouse_table_name: 'biai.patients_abc123' }
      ]
    } as any)
    mockQuery.mockResolvedValueOnce({ 
      json: async () => [
        {
          table_id: 'test_table',
          foreign_key: 'patient_id',
          referenced_table: 'patients',
          referenced_column: 'patient_id',
          relationship_type: 'many-to-one'
        }
      ]
    } as any)

    mockQuery.mockResolvedValueOnce({
      json: async () => [{ name: 'status' }, { name: 'type' }]
    } as any) // valid columns
    mockQuery.mockResolvedValueOnce({ json: async () => [{ total_count: 50 }] } as any) // total count (distinct patients)
    
    // Top categories with parent counting
    mockQuery.mockResolvedValueOnce({
      json: async () => [{ val: 'Active', cnt: 30 }, { val: 'Inactive', cnt: 20 }]
    } as any)
    mockQuery.mockResolvedValueOnce({
      json: async () => [{ val: 'Primary', cnt: 35 }, { val: 'Secondary', cnt: 15 }]
    } as any)
    
    // Cross-tab with parent counting
    mockQuery.mockResolvedValueOnce({
      json: async () => [
        { x: 'Active', y: 'Primary', count: 25 },
        { x: 'Active', y: 'Secondary', count: 5 },
        { x: 'Inactive', y: 'Primary', count: 10 },
        { x: 'Inactive', y: 'Secondary', count: 10 }
      ]
    } as any)

    const result = await aggregationService.getBivariateAggregation(
      'test_dataset',
      'test_table', 
      'status',
      'type',
      [],
      { mode: 'parent', target_table: 'patients' }
    )

    expect(result.metric_type).toBe('parent')
    expect(result.total_rows).toBe(50) // distinct patients, not total samples
    expect(result.data).toHaveLength(4) // All combinations accounted for

    // Verify parent counting queries were used (uniq instead of count)
    const queryCalls = mockQuery.mock.calls.map(call => call[0].query)
    expect(queryCalls.some(query => query.includes('uniq(') && query.includes('patient_id'))).toBe(true)
  })

  test('getBivariateAggregation handles empty results gracefully', async () => {
    const mockTableData = [mockBivariateTableData]
    
    // Setup mocks for empty result scenario
    mockQuery.mockResolvedValueOnce({ json: async () => mockTableData } as any) // table lookup
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // list columns
    // Table metadata loading with proper structure
    mockQuery.mockResolvedValueOnce({ 
      json: async () => [
        { table_id: 'test_table', table_name: 'samples', clickhouse_table_name: 'biai.samples_abc123' }
      ] 
    } as any) 
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // relationships
    mockQuery.mockResolvedValueOnce({
      json: async () => [{ name: 'status' }, { name: 'type' }]
    } as any) // valid columns
    mockQuery.mockResolvedValueOnce({ json: async () => [{ total_count: 0 }] } as any) // total count
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // top X (empty)
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // top Y (empty)  
    mockQuery.mockResolvedValueOnce({ json: async () => [] } as any) // cross-tab (empty)

    const result = await aggregationService.getBivariateAggregation(
      'test_dataset', 'test_table', 'status', 'type'
    )

    expect(result.data).toEqual([])
    expect(result.x_categories).toEqual([])
    expect(result.y_categories).toEqual([])
    expect(result.total_rows).toBe(0)
  })
})

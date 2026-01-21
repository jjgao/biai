import { describe, test, expect, beforeEach, vi } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import DatasetExplorer, {
  migrateFiltersToCurrentSchema,
  persistChartOverrides,
  loadChartOverrides
} from '../DatasetExplorer'
import api from '../../services/api'
import type { Filter } from '../../utils/filterHelpers'

// Mock the API module
vi.mock('../../services/api', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
    delete: vi.fn(),
  },
}))

// Mock react-router-dom hooks
const mockNavigate = vi.fn()
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useParams: () => ({ id: 'test-dataset-id' }),
    useNavigate: () => mockNavigate,
    useLocation: () => ({ pathname: '/datasets/test-dataset-id', search: '', hash: '' }),
  }
})

// Mock Plotly to avoid rendering issues in tests
vi.mock('react-plotly.js', () => ({
  default: () => <div data-testid="mock-plot">Mock Plot</div>,
}))

// Mock SafeHtml component
vi.mock('../../components/SafeHtml', () => ({
  default: ({ html }: { html: string }) => <div dangerouslySetInnerHTML={{ __html: html }} />,
}))

describe('DatasetExplorer', () => {
  const mockDataset = {
    id: 'test-dataset-id',
    name: 'Test Dataset',
    description: 'Test dataset description',
    database_type: 'created',
    tables: [
      {
        id: 'table1',
        name: 'customers',
        displayName: 'Customers',
        rowCount: 100,
        columns: [
          { name: 'id', type: 'integer', nullable: false },
          { name: 'name', type: 'string', nullable: false },
          { name: 'age', type: 'integer', nullable: true },
        ],
        relationships: [
          {
            foreign_key: 'region_id',
            referenced_table: 'regions',
            referenced_column: 'id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: 'table2',
        name: 'orders',
        displayName: 'Orders',
        rowCount: 500,
        columns: [
          { name: 'id', type: 'integer', nullable: false },
          { name: 'customer_id', type: 'integer', nullable: false },
          { name: 'amount', type: 'number', nullable: false },
        ],
        relationships: [
          {
            foreign_key: 'customer_id',
            referenced_table: 'customers',
            referenced_column: 'id',
            type: 'many-to-one',
          },
        ],
      },
      {
        id: 'table3',
        name: 'regions',
        displayName: 'Regions',
        rowCount: 50,
        columns: [
          { name: 'id', type: 'integer', nullable: false },
          { name: 'name', type: 'string', nullable: false },
        ],
        relationships: [],
      },
    ],
  }

  const baseAggregations = [
    {
      column_name: 'age',
      display_type: 'numeric',
      total_rows: 100,
      null_count: 5,
      unique_count: 45,
      histogram: [
        { bin_start: 0, bin_end: 10, count: 20, percentage: 20 },
        { bin_start: 10, bin_end: 20, count: 30, percentage: 30 },
      ],
    },
    {
      column_name: 'country',
      display_type: 'categorical',
      total_rows: 100,
      null_count: 0,
      unique_count: 3,
      categories: [
        { value: 'USA', display_value: 'USA', count: 50, percentage: 50 },
        { value: 'Canada', display_value: 'Canada', count: 30, percentage: 30 },
        { value: 'UK', display_value: 'UK', count: 20, percentage: 20 },
      ],
    },
  ]

  type TestPathSegment = {
    from_table: string
    via_column: string
    to_table: string
  }

  const customerPath: TestPathSegment[] = [
    { from_table: 'orders', via_column: 'customer_id', to_table: 'customers' }
  ]

  const regionPath: TestPathSegment[] = [
    { from_table: 'orders', via_column: 'customer_id', to_table: 'customers' },
    { from_table: 'customers', via_column: 'region_id', to_table: 'regions' }
  ]

  const buildAggregations = (
    metricType: 'rows' | 'parent',
    parentTable?: string,
    path?: TestPathSegment[]
  ) => {
    const parentColumn = path && path.length > 0 ? path[path.length - 1].via_column : undefined
    return baseAggregations.map(agg => ({
      ...agg,
      metric_type: metricType,
      metric_parent_table: metricType === 'parent' ? parentTable : undefined,
      metric_parent_column: metricType === 'parent' ? parentColumn : undefined,
      metric_path: metricType === 'parent' ? path : undefined
    }))
  }

  const mockColumnMetadata = [
    {
      column_name: 'age',
      column_type: 'integer',
      column_index: 2,
      is_nullable: true,
      display_name: 'Age',
      description: 'Customer age',
      user_data_type: 'numeric',
      user_priority: 1,
      display_type: 'numeric',
      unique_value_count: 45,
      null_count: 5,
      min_value: '18',
      max_value: '90',
      suggested_chart: 'histogram',
      display_priority: 1,
      is_hidden: false,
    },
    {
      column_name: 'country',
      column_type: 'string',
      column_index: 3,
      is_nullable: false,
      display_name: 'Country',
      description: 'Customer country',
      user_data_type: 'categorical',
      user_priority: 2,
      display_type: 'categorical',
      unique_value_count: 3,
      null_count: 0,
      min_value: null,
      max_value: null,
      suggested_chart: 'pie',
      display_priority: 2,
      is_hidden: false,
    },
  ]

  beforeEach(() => {
    // Clear all mocks before each test
    vi.clearAllMocks()

    // Clear localStorage
    localStorage.clear()

    // Reset navigate mock
    mockNavigate.mockClear()

    // Setup default API responses
    vi.mocked(api.get).mockImplementation((url: string, config?: { params?: Record<string, any> }) => {
      if (url === '/datasets/test-dataset-id') {
        return Promise.resolve({ data: { dataset: mockDataset } })
      }
      if (url.includes('/aggregations')) {
        const countByParam = config?.params?.countBy as string | undefined
        if (countByParam === 'parent:regions') {
          return Promise.resolve({ data: { aggregations: buildAggregations('parent', 'regions', regionPath) } })
        }
        if (countByParam === 'parent:customers') {
          return Promise.resolve({ data: { aggregations: buildAggregations('parent', 'customers', customerPath) } })
        }
        return Promise.resolve({ data: { aggregations: buildAggregations('rows') } })
      }
      if (url.includes('/columns')) {
        return Promise.resolve({ data: { columns: mockColumnMetadata } })
      }
      if (url.includes('/dashboards')) {
        // Return empty dashboards array for tests
        return Promise.resolve({ data: { dashboards: [] } })
      }
      return Promise.reject(new Error(`Unknown endpoint: ${url}`))
    })

    // Mock dashboard API POST endpoint (for saving dashboards)
    vi.mocked(api.post as any).mockImplementation((url: string) => {
      if (url.includes('/dashboards')) {
        return Promise.resolve({ data: { success: true } })
      }
      return Promise.reject(new Error(`Unknown endpoint: ${url}`))
    })

    // Mock dashboard API DELETE endpoint
    vi.mocked(api.delete as any).mockImplementation((url: string) => {
      if (url.includes('/dashboards')) {
        return Promise.resolve({ data: { success: true } })
      }
      return Promise.reject(new Error(`Unknown endpoint: ${url}`))
    })
  })

  const renderExplorer = () => {
    render(
      <BrowserRouter>
        <DatasetExplorer />
      </BrowserRouter>
    )
  }

  const activateOrdersTab = async (): Promise<HTMLSelectElement> => {
    await waitFor(() => {
      expect(screen.getByText('Test Dataset')).toBeInTheDocument()
    })
    const ordersTab = screen.getByRole('button', { name: /Orders/i })
    fireEvent.click(ordersTab)
    const countSelect = await screen.findByLabelText<HTMLSelectElement>(/Count by for Orders/i)
    return countSelect
  }

  const getOrderAggregationCallCount = () =>
    vi.mocked(api.get).mock.calls.filter(([url]) =>
      typeof url === 'string' && url.includes('/tables/table2/aggregations')
    ).length

  describe('Smoke Test', () => {
    test('renders without crashing with mock dataset', async () => {
      render(
        <BrowserRouter>
          <DatasetExplorer />
        </BrowserRouter>
      )

      // Wait for dataset to load
      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Verify tables are present (using getAllByText since tabs now also show table names)
      // Use regex to handle chart count badges like "Customers (2)"
      const customersElements = screen.getAllByText(/Customers/)
      expect(customersElements.length).toBeGreaterThan(0)

      const ordersElements = screen.getAllByText(/Orders/)
      expect(ordersElements.length).toBeGreaterThan(0)
    })
  })

  describe('Filter Persistence', () => {
    test('saves filters to localStorage when filters change', async () => {
      render(
        <BrowserRouter>
          <DatasetExplorer />
        </BrowserRouter>
      )

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Simulate adding a filter (this would normally happen through chart interaction)
      // For now, we verify localStorage is set up correctly
      const storedFilters = localStorage.getItem('filters_test-dataset-id')

      // Initially should be null or empty
      expect(storedFilters).toBeNull()
    })

    test('updates URL hash when filters change', async () => {
      render(
        <BrowserRouter>
          <DatasetExplorer />
        </BrowserRouter>
      )

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Verify navigate was not called initially (no filters)
      // In the actual implementation, navigate would be called when filters are added
      // This test validates the initial state
      expect(mockNavigate).not.toHaveBeenCalled()
    })

    test('restores filters from URL hash on mount', async () => {
      // This test verifies the deserialization logic works
      // Create a filter and encode it (same format as the app uses)
      const testFilter = [{ column: 'age', operator: 'gte' as const, value: 25, tableName: 'customers' }]
      const encoded = btoa(encodeURIComponent(JSON.stringify(testFilter)))

      // Verify encoding/decoding works correctly (unit test style)
      const decoded = JSON.parse(decodeURIComponent(atob(encoded)))
      expect(decoded).toEqual(testFilter)

      // In a full E2E test, you would navigate to /#filters=${encoded}
      // For this unit test, we verify the serialization logic
      expect(decoded[0].column).toBe('age')
      expect(decoded[0].value).toBe(25)
    })

    test('serializes and deserializes NOT wrapped filters correctly', () => {
      // Create a NOT wrapped filter
      const notFilter = [
        {
          not: {
            column: 'age',
            operator: 'in' as const,
            value: [30, 40, 50],
            tableName: 'customers'
          }
        }
      ]

      // Serialize (same as the app does)
      const encoded = btoa(encodeURIComponent(JSON.stringify(notFilter)))

      // Deserialize
      const decoded = JSON.parse(decodeURIComponent(atob(encoded)))

      // Verify NOT wrapper is preserved
      expect(decoded).toHaveLength(1)
      expect(decoded[0].not).toBeDefined()
      expect(decoded[0].not.column).toBe('age')
      expect(decoded[0].not.operator).toBe('in')
      expect(decoded[0].not.value).toEqual([30, 40, 50])
      expect(decoded[0].not.tableName).toBe('customers')
    })

    test('serializes and deserializes nested NOT with OR filters correctly', () => {
      // Create a NOT wrapper around OR combination
      const complexFilter = [
        {
          not: {
            or: [
              { column: 'age', operator: 'gte' as const, value: 60, tableName: 'patients' },
              { column: 'radiation_therapy', operator: 'eq' as const, value: 'Yes', tableName: 'patients' }
            ],
            tableName: 'patients'
          }
        }
      ]

      // Serialize
      const encoded = btoa(encodeURIComponent(JSON.stringify(complexFilter)))

      // Deserialize
      const decoded = JSON.parse(decodeURIComponent(atob(encoded)))

      // Verify complex structure is preserved
      expect(decoded[0].not).toBeDefined()
      expect(decoded[0].not.or).toHaveLength(2)
      expect(decoded[0].not.or[0].column).toBe('age')
      expect(decoded[0].not.or[1].column).toBe('radiation_therapy')
      expect(decoded[0].not.tableName).toBe('patients')
    })

    test('serializes NOT filters with countByKey metadata', () => {
      // NOT filter with countByKey (client-side metadata from PR #62)
      const notFilterWithKey = [
        {
          not: {
            column: 'sample_type',
            operator: 'eq' as const,
            value: 'Primary',
            tableName: 'samples',
            countByKey: 'parent:patients'
          }
        }
      ]

      // Serialize
      const encoded = btoa(encodeURIComponent(JSON.stringify(notFilterWithKey)))

      // Deserialize
      const decoded = JSON.parse(decodeURIComponent(atob(encoded)))

      // Verify countByKey is preserved along with NOT wrapper
      expect(decoded[0].not.countByKey).toBe('parent:patients')
      expect(decoded[0].not.column).toBe('sample_type')
    })
  })

  describe('Filter Presets', () => {
    test('saves new filter preset to localStorage', async () => {
      render(
        <BrowserRouter>
          <DatasetExplorer />
        </BrowserRouter>
      )

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Initially no presets
      const storedPresets = localStorage.getItem('presets_test-dataset-id')
      expect(storedPresets).toBeNull()

      // In actual implementation, this would test the save preset button click
      // For now, verify initial state
    })

    test('loads filter preset and applies filters', async () => {
      // Pre-populate localStorage with a preset
      const mockPreset = {
        id: 'preset1',
        name: 'Test Preset',
        filters: [{ column: 'age', operator: 'gte' as const, value: 25, tableName: 'customers' }],
        createdAt: new Date().toISOString(),
      }
      localStorage.setItem('presets_test-dataset-id', JSON.stringify([mockPreset]))

      render(
        <BrowserRouter>
          <DatasetExplorer />
        </BrowserRouter>
      )

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Verify preset was loaded
      const storedPresets = localStorage.getItem('presets_test-dataset-id')
      expect(storedPresets).toBeTruthy()
      const presets = JSON.parse(storedPresets!)
      expect(presets).toHaveLength(1)
      expect(presets[0].name).toBe('Test Preset')
    })

    test('deletes filter preset from localStorage', async () => {
      // Pre-populate localStorage with presets
      const mockPresets = [
        {
          id: 'preset1',
          name: 'Preset 1',
          filters: [],
          createdAt: new Date().toISOString(),
        },
        {
          id: 'preset2',
          name: 'Preset 2',
          filters: [],
          createdAt: new Date().toISOString(),
        },
      ]
      localStorage.setItem('presets_test-dataset-id', JSON.stringify(mockPresets))

      render(
        <BrowserRouter>
          <DatasetExplorer />
        </BrowserRouter>
      )

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Verify presets were loaded
      const storedPresets = localStorage.getItem('presets_test-dataset-id')
      const presets = JSON.parse(storedPresets!)
      expect(presets).toHaveLength(2)

      // In actual implementation, would test delete button click
      // For now, verify presets are loaded
    })

    // TODO: Skip until count-by UI tests are rewritten - see issue #101
    test.skip('applying a preset restores count-by selections', async () => {
      const presetWithAncestor = {
        id: 'preset-countBy',
        name: 'Parent Count',
        filters: [{ column: 'amount', operator: 'gte' as const, value: 100, tableName: 'orders' }],
        countBySelections: {
          orders: { mode: 'parent' as const, targetTable: 'customers' }
        },
        createdAt: new Date().toISOString()
      }
      localStorage.setItem('presets_test-dataset-id', JSON.stringify([presetWithAncestor]))

      renderExplorer()

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      const loadButton = screen.getByText('Load Filter')
      fireEvent.click(loadButton)

      const presetOption = await screen.findByText('Parent Count')
      fireEvent.click(presetOption)

      const countSelect = await activateOrdersTab()
      expect(countSelect.value).toBe('parent:customers')
    })

    test('applying a preset with NOT filters preserves NOT wrapper', async () => {
      const presetWithNOT = {
        id: 'preset-not',
        name: 'NOT Filter Test',
        filters: [
          {
            not: {
              column: 'age',
              operator: 'in' as const,
              value: [30, 40, 50],
              tableName: 'customers'
            }
          }
        ],
        countBySelections: {},
        createdAt: new Date().toISOString()
      }
      localStorage.setItem('presets_test-dataset-id', JSON.stringify([presetWithNOT]))

      renderExplorer()

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Verify NOT filter structure is preserved in storage
      const storedPresets = localStorage.getItem('presets_test-dataset-id')
      expect(storedPresets).toBeTruthy()
      const presets = JSON.parse(storedPresets!)
      expect(presets[0].filters[0].not).toBeDefined()
      expect(presets[0].filters[0].not.column).toBe('age')
      expect(presets[0].filters[0].not.operator).toBe('in')
      expect(presets[0].filters[0].not.value).toEqual([30, 40, 50])
    })
  })

  describe('View Preferences', () => {
    test('toggles between chart and table view and persists to localStorage', async () => {
      renderExplorer()

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      // Initially no view preferences stored
      const storedPrefs = localStorage.getItem('viewPrefs_test-dataset-id')
      expect(storedPrefs).toBeNull()

      // In actual implementation, would test toggle button click
      // For now, verify initial state
    })
  })

  // TODO: Count By controls tests need to be rewritten for new per-chart button UI
  // The UI now uses buttons per chart instead of a table-level select dropdown
  // See: https://github.com/jjgao/biai/issues/101
  describe('Count By controls', () => {
    test.skip('shows multi-hop parent options and renders ancestor badges', async () => {
      renderExplorer()

      const countSelect = await activateOrdersTab()

      const optionLabels = Array.from(countSelect.querySelectorAll('option')).map(option => option.textContent)
      expect(optionLabels).toEqual(expect.arrayContaining([
        'Customers',
        'Regions'
      ]))

      fireEvent.change(countSelect, { target: { value: 'parent:regions' } })

      await waitFor(() => {
        expect(countSelect.value).toBe('parent:regions')
      })

    })

    test.skip('reuses cached aggregations when toggling count targets', async () => {
      renderExplorer()

      const countSelect = await activateOrdersTab()
      const initialCalls = getOrderAggregationCallCount()

      fireEvent.change(countSelect, { target: { value: 'parent:customers' } })

      await waitFor(() => {
        expect(countSelect.value).toBe('parent:customers')
      })

      await waitFor(() => {
        expect(getOrderAggregationCallCount()).toBe(initialCalls + 1)
      })

      fireEvent.change(countSelect, { target: { value: 'rows' } })
      await waitFor(() => {
        expect(countSelect.value).toBe('rows')
      })
      expect(getOrderAggregationCallCount()).toBe(initialCalls + 1)

      fireEvent.change(countSelect, { target: { value: 'parent:customers' } })
      await waitFor(() => {
        expect(countSelect.value).toBe('parent:customers')
      })
      expect(getOrderAggregationCallCount()).toBe(initialCalls + 1)
    })

    test.skip('shows pie percentage toggle for parent metrics', async () => {
      renderExplorer()

      const countSelect = await activateOrdersTab()
      fireEvent.change(countSelect, { target: { value: 'parent:regions' } })

      await waitFor(() => {
        expect(countSelect.value).toBe('parent:regions')
      })

      const settingsButton = screen.getByRole('button', { name: /Chart settings/i })
      fireEvent.click(settingsButton)

      const percentagesBtn = screen.getByRole('button', { name: 'Percentages' })
      fireEvent.click(percentagesBtn)
      await waitFor(() => {
        expect(localStorage.getItem('chartLabels_test-dataset-id')).toBe('percent')
      })
    })
  })

  // TODO: Dashboard integration tests need to be rewritten for new per-chart button UI
  // See: https://github.com/jjgao/biai/issues/101
  describe('Dashboard integration', () => {
    test.skip('pins charts with the selected count-by target', async () => {
      renderExplorer()

      const countSelect = await activateOrdersTab()
      fireEvent.change(countSelect, { target: { value: 'parent:regions' } })

      await waitFor(() => {
        expect(countSelect.value).toBe('parent:regions')
      })

      await waitFor(() => {
        expect(screen.getAllByTitle('Add to dashboard').length).toBeGreaterThan(0)
      })
      const addButton = screen.getAllByTitle('Add to dashboard')[0]
      fireEvent.click(addButton)

      const dashboardTab = screen.getByRole('button', { name: /Dashboard/i })
      fireEvent.click(dashboardTab)

      await waitFor(() => {
        const headings = screen.getAllByTitle((value, element) =>
          element !== null && element.tagName === 'H4' && value.includes('Regions via orders.customer_id → customers.region_id')
        )
        expect(headings.length).toBeGreaterThan(0)
      })
    })

    test.skip('dashboard chart tooltips include ancestor path', async () => {
      renderExplorer()

      const countSelect = await activateOrdersTab()
      fireEvent.change(countSelect, { target: { value: 'parent:customers' } })

      await waitFor(() => {
        expect(countSelect.value).toBe('parent:customers')
      })

      const addButton = (await screen.findAllByTitle('Add to dashboard'))[0]
      fireEvent.click(addButton)

      const dashboardTab = screen.getByRole('button', { name: /Dashboard/i })
      fireEvent.click(dashboardTab)

      await waitFor(() => {
        const headings = screen.getAllByTitle((value, element) =>
          element !== null && element.tagName === 'H4' && value.includes('Customers via orders.customer_id')
        )
        expect(headings.length).toBeGreaterThan(0)
      })
    })
  })

  describe('Filter migration utilities', () => {
    test('migrates legacy filters to include row count keys', () => {
      const legacyFilters: Filter[] = [
        { column: 'age', operator: 'gte', value: 40, tableName: 'customers' },
        { column: 'amount', operator: 'lt', value: 1000, tableName: 'orders', not: { column: 'amount', operator: 'lt', value: 1000, tableName: 'orders' } }
      ]

      const migrated = migrateFiltersToCurrentSchema(legacyFilters)

      expect(migrated).toHaveLength(legacyFilters.length)
      migrated.forEach(filter => {
        const actual = filter.not ?? filter
        expect(actual?.countByKey).toBe('rows')
      })

      // Ensure original references are not mutated
      expect(legacyFilters[0].countByKey).toBeUndefined()
    })
  })

  describe('Chart override persistence', () => {
    test('persist/load helpers round-trip overrides via storage', () => {
      const storageKey = 'chartOverrides_unit-test'
      localStorage.removeItem(storageKey)

      persistChartOverrides(localStorage, 'unit-test', { 'customers.age': 'parent:regions' })

      expect(localStorage.getItem(storageKey)).toBe('{"customers.age":"parent:regions"}')

      const loaded = loadChartOverrides(localStorage, 'unit-test')
      expect(loaded).toEqual({ 'customers.age': 'parent:regions' })

      persistChartOverrides(localStorage, 'unit-test', {})
      expect(localStorage.getItem(storageKey)).toBeNull()
    })

    test('loads overrides from localStorage and requests ancestor aggregations', async () => {
      localStorage.setItem('chartOverrides_test-dataset-id', JSON.stringify({ 'customers.age': 'parent:regions' }))

      renderExplorer()

      await waitFor(() => {
        expect(screen.getByText('Test Dataset')).toBeInTheDocument()
      })

      await waitFor(() => {
        const hasParentRequest = vi.mocked(api.get).mock.calls.some(([url, config]) => {
          const countBy = (config as { params?: Record<string, any> } | undefined)?.params?.countBy
          return typeof url === 'string' && url.includes('/tables/table1/aggregations') && countBy === 'parent:regions'
        })
        expect(hasParentRequest).toBe(true)
      })
    })
  })
})

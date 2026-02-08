import { describe, test, expect, vi } from 'vitest'
import { screen } from '@testing-library/react'
import type { ColumnAggregation } from '../../types'
import { PieChart } from '../PieChart'
import { renderWithChartContext } from './testUtils'

// Mock react-plotly.js to avoid Plotly dependency in tests
vi.mock('react-plotly.js', () => ({
  default: (props: Record<string, unknown>) => <div data-testid="plotly-chart" data-type={String((props.data as Array<{ type: string }>)?.[0]?.type)} />,
}))

describe('PieChart', () => {
  const mockAggregation: ColumnAggregation = {
    column_name: 'status',
    display_type: 'categorical',
    null_count: 0,
    unique_count: 3,
    categories: [
      { value: 'Active', display_value: 'Active', count: 50, percentage: 50 },
      { value: 'Inactive', display_value: 'Inactive', count: 30, percentage: 30 },
      { value: 'Pending', display_value: 'Pending', count: 20, percentage: 20 },
    ],
    total_rows: 100,
  }

  test('shows loading state when no aggregation data', () => {
    renderWithChartContext(
      <PieChart title="Status" tableName="users" field="status" />
    )

    expect(screen.getByText('Loading data…')).toBeTruthy()
  })

  test('renders chart with aggregation data', () => {
    renderWithChartContext(
      <PieChart
        title="Status"
        tableName="users"
        field="status"
        aggregationOverride={mockAggregation}
        cacheKeyOverride="rows"
      />,
      {
        getColumnMetadata: vi.fn(() => ({ display_name: 'User Status' }) as never),
        getBaselineAggregation: vi.fn(() => mockAggregation),
      }
    )

    expect(screen.getByText('User Status')).toBeTruthy()
    expect(screen.getByTestId('plotly-chart')).toBeTruthy()
  })

  test('renders chart container with correct dimensions', () => {
    const { container } = renderWithChartContext(
      <PieChart
        title="Status"
        tableName="users"
        field="status"
        tableColor="#4CAF50"
        aggregationOverride={mockAggregation}
        cacheKeyOverride="rows"
      />,
      {
        getBaselineAggregation: vi.fn(() => mockAggregation),
      }
    )

    const wrapper = container.firstElementChild as HTMLElement
    expect(wrapper?.style.width).toBe('175px')
    expect(wrapper?.style.minHeight).toBe('175px')
  })
})

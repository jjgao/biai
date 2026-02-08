import { describe, test, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ActiveFilters } from '../ActiveFilters'
import type { Filter } from '../../../../utils/filterHelpers'

const defaultProps = {
  setFilters: vi.fn(),
  onSaveFilter: vi.fn(),
  onClearFilters: vi.fn(),
  getTableColor: () => '#2196F3',
  getFilterTableNameForCacheKey: () => 'patients',
  targetFromCacheKey: () => null,
  formatRangeValue: (v: number) => v.toFixed(2),
  clearColumnFilter: vi.fn(),
  datasetTables: [{ name: 'patients', displayName: 'Patients' }],
}

describe('ActiveFilters', () => {
  test('renders nothing when no filters', () => {
    const { container } = render(<ActiveFilters filters={[]} {...defaultProps} />)
    expect(container.innerHTML).toBe('')
  })

  test('renders filter chips and action buttons', () => {
    const filters: Filter[] = [
      { column: 'age', operator: 'eq', value: '30', tableName: 'patients' },
    ]
    render(<ActiveFilters filters={filters} {...defaultProps} />)

    expect(screen.getByText('Active Filters:')).toBeTruthy()
    expect(screen.getByText('Save Filter')).toBeTruthy()
    expect(screen.getByText('Clear All')).toBeTruthy()
    expect(screen.getByText('age:')).toBeTruthy()
  })

  test('calls onSaveFilter when Save Filter is clicked', () => {
    const onSaveFilter = vi.fn()
    const filters: Filter[] = [
      { column: 'age', operator: 'eq', value: '30', tableName: 'patients' },
    ]
    render(<ActiveFilters filters={filters} {...defaultProps} onSaveFilter={onSaveFilter} />)

    fireEvent.click(screen.getByText('Save Filter'))
    expect(onSaveFilter).toHaveBeenCalled()
  })

  test('calls onClearFilters when Clear All is clicked', () => {
    const onClearFilters = vi.fn()
    const filters: Filter[] = [
      { column: 'age', operator: 'eq', value: '30', tableName: 'patients' },
    ]
    render(<ActiveFilters filters={filters} {...defaultProps} onClearFilters={onClearFilters} />)

    fireEvent.click(screen.getByText('Clear All'))
    expect(onClearFilters).toHaveBeenCalled()
  })

  test('renders AND separator between multiple filters', () => {
    const filters: Filter[] = [
      { column: 'age', operator: 'eq', value: '30', tableName: 'patients' },
      { column: 'state', operator: 'eq', value: 'CA', tableName: 'patients' },
    ]
    render(<ActiveFilters filters={filters} {...defaultProps} />)

    expect(screen.getByText('AND')).toBeTruthy()
  })
})

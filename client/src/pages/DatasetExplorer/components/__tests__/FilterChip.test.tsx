import { describe, test, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { FilterChip } from '../FilterChip'
import type { Filter } from '../../../../utils/filterHelpers'

const defaultProps = {
  index: 0,
  showAndSeparator: false,
  getTableColor: () => '#2196F3',
  getFilterTableNameForCacheKey: () => 'patients',
  targetFromCacheKey: () => null,
  formatRangeValue: (v: number) => v.toFixed(2),
  clearColumnFilter: vi.fn(),
  setFilters: vi.fn(),
  datasetTables: [{ name: 'patients', displayName: 'Patients' }],
}

describe('FilterChip', () => {
  test('renders column name and value for eq filter', () => {
    const filter: Filter = { column: 'age', operator: 'eq', value: '30', tableName: 'patients' }
    render(<FilterChip filter={filter} {...defaultProps} />)

    expect(screen.getByText('age:')).toBeTruthy()
    expect(screen.getByText('30')).toBeTruthy()
  })

  test('renders AND separator when showAndSeparator is true', () => {
    const filter: Filter = { column: 'age', operator: 'eq', value: '30', tableName: 'patients' }
    render(<FilterChip filter={filter} {...defaultProps} showAndSeparator={true} />)

    expect(screen.getByText('AND')).toBeTruthy()
  })

  test('renders NOT indicator for negated filter', () => {
    const filter: Filter = { not: { column: 'age', operator: 'eq', value: '30', tableName: 'patients' } }
    render(<FilterChip filter={filter} {...defaultProps} />)

    expect(screen.getByText('NOT')).toBeTruthy()
  })

  test('calls clearColumnFilter when remove button is clicked', () => {
    const clearColumnFilter = vi.fn()
    const filter: Filter = { column: 'age', operator: 'eq', value: '30', tableName: 'patients' }
    render(<FilterChip filter={filter} {...defaultProps} clearColumnFilter={clearColumnFilter} />)

    fireEvent.click(screen.getByText('×'))
    expect(clearColumnFilter).toHaveBeenCalled()
  })

  test('toggles NOT when toggle button is clicked', () => {
    const setFilters = vi.fn()
    const filter: Filter = { column: 'age', operator: 'eq', value: '30', tableName: 'patients' }
    render(<FilterChip filter={filter} {...defaultProps} setFilters={setFilters} />)

    fireEvent.click(screen.getByTitle('Add NOT'))
    expect(setFilters).toHaveBeenCalled()
  })

  test('renders multi-value display for in operator', () => {
    const filter: Filter = { column: 'state', operator: 'in', value: ['CA', 'NY', 'TX'], tableName: 'patients' }
    render(<FilterChip filter={filter} {...defaultProps} />)

    expect(screen.getByText(/CA OR NY OR TX/)).toBeTruthy()
  })
})

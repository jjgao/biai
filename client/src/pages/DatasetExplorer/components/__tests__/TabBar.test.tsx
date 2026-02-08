import { describe, test, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { TabBar } from '../TabBar'

const defaultProps = {
  tables: [
    { name: 'patients', displayName: 'Patients' },
    { name: 'visits', displayName: 'Visits' },
  ],
  activeTab: null as string | null,
  onTabChange: vi.fn(),
  dashboardChartCount: 0,
  getTableColor: () => '#2196F3',
  getTableChartCount: () => 3,
}

describe('TabBar', () => {
  test('renders dashboard tab', () => {
    render(<TabBar {...defaultProps} />)
    expect(screen.getByText(/Dashboard/)).toBeTruthy()
  })

  test('renders table tabs', () => {
    render(<TabBar {...defaultProps} />)
    expect(screen.getByText(/Patients/)).toBeTruthy()
    expect(screen.getByText(/Visits/)).toBeTruthy()
  })

  test('calls onTabChange with dashboard when dashboard tab is clicked', () => {
    const onTabChange = vi.fn()
    render(<TabBar {...defaultProps} onTabChange={onTabChange} activeTab="patients" />)

    fireEvent.click(screen.getByText(/Dashboard/))
    expect(onTabChange).toHaveBeenCalledWith('dashboard')
  })

  test('calls onTabChange with table name when table tab is clicked', () => {
    const onTabChange = vi.fn()
    render(<TabBar {...defaultProps} onTabChange={onTabChange} />)

    fireEvent.click(screen.getByText(/Patients/))
    expect(onTabChange).toHaveBeenCalledWith('patients')
  })

  test('shows dashboard chart count when charts exist', () => {
    render(<TabBar {...defaultProps} dashboardChartCount={5} />)
    expect(screen.getByText(/Dashboard.*5/)).toBeTruthy()
  })

  test('uses table name when displayName is not provided', () => {
    render(
      <TabBar
        {...defaultProps}
        tables={[{ name: 'raw_data' }]}
      />
    )
    expect(screen.getByText(/raw_data/)).toBeTruthy()
  })
})

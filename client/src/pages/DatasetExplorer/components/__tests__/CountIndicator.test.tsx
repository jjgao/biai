import { describe, test, expect, vi } from 'vitest'
import { screen, fireEvent } from '@testing-library/react'
import { CountIndicator } from '../CountIndicator'
import { renderWithChartContext } from './testUtils'

describe('CountIndicator', () => {
  const defaultProps = {
    menuKey: 'table:test.col',
    indicatorColor: '#2196F3',
    borderColor: null as string | null,
    label: 'Rows',
    options: [
      { value: 'rows', label: 'Rows' },
      { value: 'parent:patients', label: 'Patients' },
    ],
    currentValue: 'rows',
    onSelect: vi.fn(),
    buttonLabel: 'Change count-by',
  }

  test('renders the color indicator button', () => {
    renderWithChartContext(<CountIndicator {...defaultProps} />)

    const button = screen.getByLabelText('Change count-by')
    expect(button).toBeTruthy()
    expect(button.style.background).toBe('rgb(33, 150, 243)')
  })

  test('opens dropdown on click when activeCountMenuKey matches', () => {
    const setActiveCountMenuKey = vi.fn()
    renderWithChartContext(
      <CountIndicator {...defaultProps} />,
      { activeCountMenuKey: 'table:test.col', setActiveCountMenuKey }
    )

    // When the menu key matches activeCountMenuKey, the dropdown should be visible
    expect(screen.getByText('Rows')).toBeTruthy()
    expect(screen.getByText('Patients')).toBeTruthy()
  })

  test('calls onSelect when an option is clicked', () => {
    const onSelect = vi.fn()
    const setActiveCountMenuKey = vi.fn()
    renderWithChartContext(
      <CountIndicator {...defaultProps} onSelect={onSelect} />,
      { activeCountMenuKey: 'table:test.col', setActiveCountMenuKey }
    )

    fireEvent.click(screen.getByText('Patients'))
    expect(onSelect).toHaveBeenCalledWith('parent:patients')
  })

  test('renders wider button when borderColor differs from indicatorColor', () => {
    renderWithChartContext(
      <CountIndicator {...defaultProps} borderColor="#4CAF50" />
    )

    const button = screen.getByLabelText('Change count-by')
    // When borderColor is set, the button gets a wider width (14px vs 10px)
    expect(button.style.width).toBe('14px')
  })
})

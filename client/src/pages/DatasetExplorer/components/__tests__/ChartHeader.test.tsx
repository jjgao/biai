import { describe, test, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ChartHeader } from '../ChartHeader'

describe('ChartHeader', () => {
  test('renders title and count indicator', () => {
    render(
      <ChartHeader
        title="Age Distribution"
        countIndicator={<span data-testid="indicator">●</span>}
      />
    )

    expect(screen.getByText('Age Distribution')).toBeTruthy()
    expect(screen.getByTestId('indicator')).toBeTruthy()
  })

  test('renders actions when provided', () => {
    render(
      <ChartHeader
        title="Test"
        countIndicator={<span />}
        actions={<button type="button">Filter</button>}
      />
    )

    expect(screen.getByText('Filter')).toBeTruthy()
  })

  test('shows list column indicator when isListColumn is true', () => {
    render(
      <ChartHeader
        title="Tags"
        countIndicator={<span />}
        isListColumn={true}
      />
    )

    expect(screen.getByTitle('List column - items can appear in multiple rows')).toBeTruthy()
  })

  test('sets tooltip on title element', () => {
    render(
      <ChartHeader
        title="State"
        tooltip="Geographic state field"
        countIndicator={<span />}
      />
    )

    expect(screen.getByTitle('Geographic state field')).toBeTruthy()
  })
})

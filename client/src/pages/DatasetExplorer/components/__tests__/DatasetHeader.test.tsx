import { describe, test, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { DatasetHeader } from '../DatasetHeader'

describe('DatasetHeader', () => {
  test('renders dataset name', () => {
    render(<DatasetHeader name="Test Dataset" tableCount={3} onManage={vi.fn()} />)
    expect(screen.getByText('Test Dataset')).toBeTruthy()
  })

  test('renders table count', () => {
    render(<DatasetHeader name="Test" tableCount={5} onManage={vi.fn()} />)
    expect(screen.getByText('5')).toBeTruthy()
  })

  test('renders description when provided', () => {
    render(
      <DatasetHeader
        name="Test"
        description="<p>A description</p>"
        tableCount={2}
        onManage={vi.fn()}
      />
    )
    expect(screen.getByText('A description')).toBeTruthy()
  })

  test('does not render description when not provided', () => {
    const { container } = render(
      <DatasetHeader name="Test" tableCount={2} onManage={vi.fn()} />
    )
    expect(container.querySelectorAll('p').length).toBe(0)
  })

  test('calls onManage when manage button is clicked', () => {
    const onManage = vi.fn()
    render(<DatasetHeader name="Test" tableCount={2} onManage={onManage} />)

    fireEvent.click(screen.getByTitle('Manage dataset'))
    expect(onManage).toHaveBeenCalledOnce()
  })
})

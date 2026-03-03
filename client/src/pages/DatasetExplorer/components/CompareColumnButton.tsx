import { useState, useRef, useEffect } from 'react'
import { useChartContext } from './ChartContext'

interface CompareColumnButtonProps {
  tableName: string
  columnName: string
}

/**
 * "Compare" button (⊕) that opens a dropdown of categorical columns
 * from the same table. Selecting a column creates a bivariate (stacked bar)
 * chart pairing.
 */
export function CompareColumnButton({ tableName, columnName }: CompareColumnButtonProps) {
  const ctx = useChartContext()
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  // Guard: Only show for categorical columns (safety net for future phases)
  const primaryColumnMetadata = ctx.getColumnMetadata(tableName, columnName)
  if (!primaryColumnMetadata || 
      (primaryColumnMetadata.display_type !== 'categorical' && primaryColumnMetadata.display_type !== 'id')) {
    return null
  }

  const currentSelection = ctx.getBivariateSelection(tableName, columnName)

  // Close dropdown on outside click
  useEffect(() => {
    if (!open) return
    const handleClick = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [open])

  const categoricalColumns = ctx.getCategoricalColumns(tableName)
    .filter(col => col.column_name !== columnName)

  if (categoricalColumns.length === 0) return null

  return (
    <div ref={containerRef} style={{ position: 'relative', display: 'inline-block' }}>
      <button
        type="button"
        onClick={event => {
          event.stopPropagation()
          setOpen(prev => !prev)
        }}
        style={{
          border: 'none',
          background: currentSelection ? '#7B1FA2' : '#f0f0f0',
          color: currentSelection ? 'white' : '#333',
          borderRadius: '50%',
          width: '20px',
          height: '20px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '0.75rem',
          cursor: 'pointer',
          lineHeight: 1,
          fontWeight: 700,
        }}
        title={currentSelection ? `Comparing with ${currentSelection}` : 'Compare with another column'}
      >
        ⊕
      </button>
      {open && (
        <div
          style={{
            position: 'absolute',
            top: '100%',
            right: 0,
            zIndex: 1000,
            background: 'white',
            border: '1px solid #ddd',
            borderRadius: '6px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            minWidth: '180px',
            maxHeight: '240px',
            overflowY: 'auto',
            padding: '0.25rem 0',
            marginTop: '4px',
          }}
        >
          <div style={{
            padding: '0.3rem 0.6rem',
            fontSize: '0.65rem',
            color: '#999',
            fontWeight: 600,
            textTransform: 'uppercase',
            letterSpacing: '0.5px',
          }}>
            Compare with
          </div>
          {categoricalColumns.map(col => (
            <button
              key={col.column_name}
              type="button"
              onClick={event => {
                event.stopPropagation()
                ctx.setBivariateSelection(tableName, columnName, col.column_name)
                setOpen(false)
              }}
              style={{
                display: 'block',
                width: '100%',
                padding: '0.35rem 0.6rem',
                background: currentSelection === col.column_name ? '#7B1FA215' : 'transparent',
                border: 'none',
                textAlign: 'left',
                cursor: 'pointer',
                fontSize: '0.75rem',
                color: '#333',
                fontWeight: currentSelection === col.column_name ? 600 : 400,
              }}
              onMouseEnter={e => {
                e.currentTarget.style.background = '#f5f5f5'
              }}
              onMouseLeave={e => {
                e.currentTarget.style.background = currentSelection === col.column_name ? '#7B1FA215' : 'transparent'
              }}
              title={col.column_name}
            >
              {col.display_name || col.column_name}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

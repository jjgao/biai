import { useChartContext } from './ChartContext'

interface CountIndicatorProps {
  menuKey: string
  indicatorColor: string
  borderColor?: string | null
  label: string
  options: Array<{ value: string; label: string }>
  currentValue: string
  onSelect: (value: string) => void
  buttonLabel: string
  size?: 'default' | 'large'
}

export function CountIndicator({
  menuKey,
  indicatorColor,
  borderColor,
  label,
  options,
  currentValue,
  onSelect,
  buttonLabel,
  size = 'default'
}: CountIndicatorProps) {
  const { activeCountMenuKey, setActiveCountMenuKey } = useChartContext()

  const isOpen = activeCountMenuKey === menuKey
  const hasBorder = borderColor && borderColor !== indicatorColor

  // Size variants
  const dimensions = size === 'large'
    ? { width: hasBorder ? '16px' : '12px', height: '40px' }
    : { width: hasBorder ? '14px' : '10px', height: '22px' }

  return (
    <div style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
      <button
        type="button"
        aria-label={buttonLabel}
        title={`${label} (click to change)`}
        onClick={event => {
          event.stopPropagation()
          setActiveCountMenuKey(prev => (prev === menuKey ? null : menuKey))
        }}
        style={{
          width: dimensions.width,
          height: dimensions.height,
          borderRadius: '4px',
          border: hasBorder ? `2px solid ${borderColor}` : 'none',
          background: indicatorColor,
          cursor: 'pointer',
          padding: 0
        }}
      />
      {isOpen && (
        <div
          style={{
            position: 'absolute',
            top: '120%',
            left: 0,
            background: 'white',
            border: '1px solid #ddd',
            borderRadius: '6px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            padding: '0.35rem 0.4rem',
            zIndex: 20,
            minWidth: '160px',
            display: 'flex',
            flexDirection: 'column',
            gap: '0.3rem'
          }}
          onClick={event => event.stopPropagation()}
        >
          {options.map(option => {
            const active = option.value === currentValue
            return (
              <button
                key={option.value}
                type="button"
                onClick={() => {
                  onSelect(option.value)
                  setActiveCountMenuKey(null)
                }}
                style={{
                  textAlign: 'left',
                  border: active ? '1px solid #1976D2' : '1px solid transparent',
                  borderRadius: '4px',
                  background: active ? '#E3F2FD' : 'transparent',
                  color: '#333',
                  fontSize: '0.72rem',
                  padding: '0.15rem 0.35rem',
                  cursor: 'pointer'
                }}
              >
                {option.label}
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}

import React from 'react'

interface ChartHeaderProps {
  title: string
  tooltip?: string
  countIndicator: React.ReactNode
  actions?: React.ReactNode
  isListColumn?: boolean
}

export function ChartHeader({ title, tooltip, countIndicator, actions, isListColumn }: ChartHeaderProps) {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '0.35rem',
        marginBottom: '0.4rem'
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', minWidth: 0, flex: 1 }}>
        {countIndicator}
        <h4
          style={{
            margin: 0,
            fontSize: '0.75rem',
            fontWeight: 600,
            cursor: tooltip ? 'help' : 'default',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            flex: 1
          }}
          title={tooltip}
        >
          {title}
          {isListColumn && (
            <span
              style={{
                marginLeft: '0.25rem',
                fontSize: '0.65rem',
                opacity: 0.7
              }}
              title="List column - items can appear in multiple rows"
            >
              📋
            </span>
          )}
        </h4>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', flexShrink: 0 }}>
        {actions}
      </div>
    </div>
  )
}

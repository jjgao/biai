import React from 'react'
import { CHART_LABEL_STORAGE_PREFIX } from '../types'

interface ChartSettingsMenuProps {
  showPercentageLabels: boolean
  setShowPercentageLabels: (value: boolean) => void
  onClose: () => void
  identifier: string | undefined
  menuRef: React.RefObject<HTMLDivElement>
}

export function ChartSettingsMenu({
  showPercentageLabels,
  setShowPercentageLabels,
  onClose,
  identifier,
  menuRef,
}: ChartSettingsMenuProps) {
  return (
    <div
      ref={menuRef}
      style={{
        position: 'absolute',
        top: 'calc(100% + 8px)',
        right: 0,
        background: 'white',
        borderRadius: '8px',
        boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
        padding: '0.75rem 1rem',
        width: '220px',
        zIndex: 5
      }}
    >
      <div style={{ fontWeight: 600, fontSize: '0.85rem', marginBottom: '0.5rem' }}>Chart settings</div>
      <div style={{ fontSize: '0.75rem', marginBottom: '0.25rem', color: '#444' }}>Chart labels</div>
      <div style={{ display: 'flex', gap: '0.35rem' }}>
        <button
          type="button"
          onClick={() => {
            setShowPercentageLabels(false)
            onClose()
            try {
              localStorage.setItem(`${CHART_LABEL_STORAGE_PREFIX}${identifier}`, 'count')
            } catch (e) {
              console.error('Failed to save chart label preference', e)
            }
          }}
          style={{
            border: 'none',
            borderRadius: '999px',
            padding: '0.2rem 0.9rem',
            fontSize: '0.75rem',
            cursor: 'pointer',
            background: showPercentageLabels ? '#ECEFF1' : '#1976D2',
            color: showPercentageLabels ? '#333' : 'white'
          }}
        >
          Counts
        </button>
        <button
          type="button"
          onClick={() => {
            setShowPercentageLabels(true)
            onClose()
            try {
              localStorage.setItem(`${CHART_LABEL_STORAGE_PREFIX}${identifier}`, 'percent')
            } catch (e) {
              console.error('Failed to save chart label preference', e)
            }
          }}
          style={{
            border: 'none',
            borderRadius: '999px',
            padding: '0.2rem 0.9rem',
            fontSize: '0.75rem',
            cursor: 'pointer',
            background: showPercentageLabels ? '#1976D2' : '#ECEFF1',
            color: showPercentageLabels ? 'white' : '#333'
          }}
        >
          Percentages
        </button>
      </div>
      <div style={{ fontSize: '0.7rem', color: '#777', marginTop: '0.4rem' }}>
        {showPercentageLabels ? 'Percentages may exceed 100% when parents overlap.' : 'Switch to percentages when needed.'}
      </div>
    </div>
  )
}

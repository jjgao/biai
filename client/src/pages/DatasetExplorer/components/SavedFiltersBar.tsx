import type { FilterPreset } from '../../../utils/presetHelpers'

interface SavedFiltersBarProps {
  presets: FilterPreset[]
  showPresetsDropdown: boolean
  setShowPresetsDropdown: (value: boolean) => void
  setShowManagePresetsDialog: (value: boolean) => void
}

export function SavedFiltersBar({
  presets,
  showPresetsDropdown,
  setShowPresetsDropdown,
  setShowManagePresetsDialog,
}: SavedFiltersBarProps) {
  if (presets.length === 0) return null

  return (
    <div style={{
      marginBottom: '1rem',
      background: '#E3F2FD',
      padding: '0.75rem 1rem',
      borderRadius: '8px',
      border: '1px solid #90CAF9',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between'
    }}>
      <div style={{ fontSize: '0.875rem', color: '#1976D2', fontWeight: 500 }}>
        {presets.length} saved filter{presets.length !== 1 ? 's' : ''}
      </div>
      <div style={{ display: 'flex', gap: '0.5rem' }}>
        <div style={{ position: 'relative' }}>
          <button
            onClick={() => setShowPresetsDropdown(!showPresetsDropdown)}
            style={{
              padding: '0.25rem 0.75rem',
              background: '#2196F3',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '0.75rem'
            }}
            title="Load a saved filter"
          >
            Load Filter
          </button>
        </div>
        <button
          onClick={() => setShowManagePresetsDialog(true)}
          style={{
            padding: '0.25rem 0.75rem',
            background: '#FF9800',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '0.75rem'
          }}
          title="Manage saved filters"
        >
          Manage
        </button>
      </div>
    </div>
  )
}

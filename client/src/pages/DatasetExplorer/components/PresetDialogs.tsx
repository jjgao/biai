import React from 'react'
import type { FilterPreset } from '../../../utils/presetHelpers'

interface PresetDialogsProps {
  presets: FilterPreset[]
  showSavePresetDialog: boolean
  setShowSavePresetDialog: (value: boolean) => void
  showPresetsDropdown: boolean
  setShowPresetsDropdown: (value: boolean) => void
  showManagePresetsDialog: boolean
  setShowManagePresetsDialog: (value: boolean) => void
  presetNameInput: string
  setPresetNameInput: (value: string) => void
  editingPresetId: string | null
  setEditingPresetId: (value: string | null) => void
  savePreset: () => void
  applyPreset: (preset: FilterPreset) => void
  deletePreset: (id: string) => void
  renamePreset: (id: string, name: string) => void
  exportPresets: () => void
  importPresets: (e: React.ChangeEvent<HTMLInputElement>) => void
}

export function PresetDialogs({
  presets,
  showSavePresetDialog,
  setShowSavePresetDialog,
  showPresetsDropdown,
  setShowPresetsDropdown,
  showManagePresetsDialog,
  setShowManagePresetsDialog,
  presetNameInput,
  setPresetNameInput,
  editingPresetId,
  setEditingPresetId,
  savePreset,
  applyPreset,
  deletePreset,
  renamePreset,
  exportPresets,
  importPresets,
}: PresetDialogsProps) {
  return (
    <>
      {/* Save Filter Dialog */}
      {showSavePresetDialog && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
          onClick={() => setShowSavePresetDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '400px',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ marginTop: 0 }}>Save Filter</h3>
            <input
              type="text"
              value={presetNameInput}
              onChange={(e) => setPresetNameInput(e.target.value)}
              placeholder="Enter filter name..."
              onKeyDown={(e) => {
                if (e.key === 'Enter') savePreset()
                if (e.key === 'Escape') setShowSavePresetDialog(false)
              }}
              autoFocus
              style={{
                width: '100%',
                padding: '0.5rem',
                marginBottom: '1rem',
                border: '1px solid #ddd',
                borderRadius: '4px',
                fontSize: '0.875rem'
              }}
            />
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowSavePresetDialog(false)}
                style={{
                  padding: '0.5rem 1rem',
                  background: '#f0f0f0',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.875rem'
                }}
              >
                Cancel
              </button>
              <button
                onClick={savePreset}
                disabled={!presetNameInput.trim()}
                style={{
                  padding: '0.5rem 1rem',
                  background: presetNameInput.trim() ? '#4CAF50' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: presetNameInput.trim() ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Load Filter Dropdown */}
      {showPresetsDropdown && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            zIndex: 999
          }}
          onClick={() => setShowPresetsDropdown(false)}
        >
          <div
            style={{
              position: 'absolute',
              top: '120px',
              right: '20px',
              background: 'white',
              border: '1px solid #ddd',
              borderRadius: '8px',
              boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
              padding: '0.5rem',
              minWidth: '300px',
              maxHeight: '400px',
              overflowY: 'auto'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ padding: '0.5rem', borderBottom: '1px solid #eee', marginBottom: '0.5rem' }}>
              <strong style={{ fontSize: '0.875rem' }}>Select Filter</strong>
            </div>
            {presets.map((preset) => (
              <div
                key={preset.id}
                onClick={() => applyPreset(preset)}
                style={{
                  padding: '0.75rem',
                  cursor: 'pointer',
                  borderRadius: '4px',
                  marginBottom: '0.25rem',
                  border: '1px solid #eee',
                  transition: 'background 0.2s'
                }}
                onMouseEnter={(e) => e.currentTarget.style.background = '#f0f0f0'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'white'}
              >
                <div style={{ fontWeight: 500, fontSize: '0.875rem', marginBottom: '0.25rem' }}>
                  {preset.name}
                </div>
                <div style={{ fontSize: '0.7rem', color: '#666' }}>
                  {preset.filters.length} filter{preset.filters.length !== 1 ? 's' : ''} · {new Date(preset.createdAt).toLocaleDateString()}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Manage Filters Dialog */}
      {showManagePresetsDialog && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
          onClick={() => setShowManagePresetsDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '500px',
              maxHeight: '600px',
              overflowY: 'auto',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0 }}>Manage Saved Filters</h3>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button
                  onClick={exportPresets}
                  style={{
                    padding: '0.4rem 0.75rem',
                    background: '#2196F3',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontSize: '0.75rem'
                  }}
                >
                  Export
                </button>
                <label style={{
                  padding: '0.4rem 0.75rem',
                  background: '#FF9800',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.75rem'
                }}>
                  Import
                  <input
                    type="file"
                    accept=".json"
                    onChange={importPresets}
                    style={{ display: 'none' }}
                  />
                </label>
              </div>
            </div>
            {presets.length === 0 ? (
              <p style={{ color: '#999', textAlign: 'center', padding: '2rem' }}>
                No filters saved yet.
              </p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {presets.map((preset) => (
                  <div
                    key={preset.id}
                    style={{
                      border: '1px solid #ddd',
                      borderRadius: '6px',
                      padding: '0.75rem'
                    }}
                  >
                    {editingPresetId === preset.id ? (
                      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.5rem' }}>
                        <input
                          type="text"
                          defaultValue={preset.name}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') renamePreset(preset.id, e.currentTarget.value)
                            if (e.key === 'Escape') setEditingPresetId(null)
                          }}
                          autoFocus
                          style={{
                            flex: 1,
                            padding: '0.25rem 0.5rem',
                            border: '1px solid #2196F3',
                            borderRadius: '4px',
                            fontSize: '0.875rem'
                          }}
                        />
                        <button
                          onClick={(e) => renamePreset(preset.id, (e.currentTarget.previousElementSibling as HTMLInputElement | null)?.value || '')}
                          style={{
                            padding: '0.25rem 0.5rem',
                            background: '#4CAF50',
                            color: 'white',
                            border: 'none',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontSize: '0.75rem'
                          }}
                        >
                          Save
                        </button>
                        <button
                          onClick={() => setEditingPresetId(null)}
                          style={{
                            padding: '0.25rem 0.5rem',
                            background: '#f0f0f0',
                            border: 'none',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontSize: '0.75rem'
                          }}
                        >
                          Cancel
                        </button>
                      </div>
                    ) : (
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                        <strong style={{ fontSize: '0.875rem' }}>{preset.name}</strong>
                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                          <button
                            onClick={() => setEditingPresetId(preset.id)}
                            style={{
                              padding: '0.25rem 0.5rem',
                              background: '#2196F3',
                              color: 'white',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: 'pointer',
                              fontSize: '0.75rem'
                            }}
                          >
                            Rename
                          </button>
                          <button
                            onClick={() => {
                              if (window.confirm(`Delete saved filter "${preset.name}"?`)) {
                                deletePreset(preset.id)
                              }
                            }}
                            style={{
                              padding: '0.25rem 0.5rem',
                              background: '#f44336',
                              color: 'white',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: 'pointer',
                              fontSize: '0.75rem'
                            }}
                          >
                            Delete
                          </button>
                        </div>
                      </div>
                    )}
                    <div style={{ fontSize: '0.75rem', color: '#666' }}>
                      {preset.filters.length} filter{preset.filters.length !== 1 ? 's' : ''} · Created {new Date(preset.createdAt).toLocaleDateString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowManagePresetsDialog(false)}
                style={{
                  padding: '0.5rem 1rem',
                  background: '#f0f0f0',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '0.875rem'
                }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}

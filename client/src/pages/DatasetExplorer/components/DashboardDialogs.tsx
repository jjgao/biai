import type { SavedDashboard } from '../types'

interface DashboardDialogsProps {
  savedDashboards: SavedDashboard[]
  activeDashboardId: string | null
  showSaveDashboardDialog: boolean
  setShowSaveDashboardDialog: (value: boolean) => void
  showLoadDashboardDialog: boolean
  setShowLoadDashboardDialog: (value: boolean) => void
  showManageDashboardsDialog: boolean
  setShowManageDashboardsDialog: (value: boolean) => void
  newDashboardName: string
  setNewDashboardName: (value: string) => void
  editingDashboardId: string | null
  setEditingDashboardId: (value: string | null) => void
  setEditingDashboardName: (value: string) => void
  saveDashboard: (name: string) => void
  loadDashboard: (id: string) => void
  deleteDashboard: (id: string) => void
  renameDashboard: (id: string, name: string) => void
}

export function DashboardDialogs({
  savedDashboards,
  activeDashboardId,
  showSaveDashboardDialog,
  setShowSaveDashboardDialog,
  showLoadDashboardDialog,
  setShowLoadDashboardDialog,
  showManageDashboardsDialog,
  setShowManageDashboardsDialog,
  newDashboardName,
  setNewDashboardName,
  editingDashboardId,
  setEditingDashboardId,
  setEditingDashboardName,
  saveDashboard,
  loadDashboard,
  deleteDashboard,
  renameDashboard,
}: DashboardDialogsProps) {
  return (
    <>
      {/* Save Dashboard Dialog */}
      {showSaveDashboardDialog && (
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
          onClick={() => setShowSaveDashboardDialog(false)}
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
            <h3 style={{ marginTop: 0 }}>Save Dashboard</h3>
            <input
              type="text"
              value={newDashboardName}
              onChange={(e) => setNewDashboardName(e.target.value)}
              placeholder="Enter dashboard name..."
              onKeyDown={(e) => {
                if (e.key === 'Enter' && newDashboardName.trim()) saveDashboard(newDashboardName.trim())
                if (e.key === 'Escape') setShowSaveDashboardDialog(false)
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
                onClick={() => {
                  setShowSaveDashboardDialog(false)
                  setNewDashboardName('')
                }}
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
                onClick={() => saveDashboard(newDashboardName.trim())}
                disabled={!newDashboardName.trim()}
                style={{
                  padding: '0.5rem 1rem',
                  background: newDashboardName.trim() ? '#4CAF50' : '#ccc',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: newDashboardName.trim() ? 'pointer' : 'not-allowed',
                  fontSize: '0.875rem'
                }}
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Load Dashboard Dialog */}
      {showLoadDashboardDialog && (
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
          onClick={() => setShowLoadDashboardDialog(false)}
        >
          <div
            style={{
              background: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              minWidth: '400px',
              maxWidth: '500px',
              maxHeight: '600px',
              overflowY: 'auto',
              boxShadow: '0 4px 12px rgba(0,0,0,0.2)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ marginTop: 0 }}>Load Dashboard</h3>
            {savedDashboards.length === 0 ? (
              <p style={{ color: '#999', textAlign: 'center', padding: '2rem' }}>
                No saved dashboards yet.
              </p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {savedDashboards.map(dashboard => (
                  <div
                    key={dashboard.id}
                    onClick={() => {
                      loadDashboard(dashboard.id)
                      setShowLoadDashboardDialog(false)
                    }}
                    style={{
                      padding: '0.75rem',
                      border: activeDashboardId === dashboard.id ? '2px solid #2196F3' : '1px solid #ddd',
                      borderRadius: '6px',
                      cursor: 'pointer',
                      transition: 'background 0.2s, border-color 0.2s',
                      background: activeDashboardId === dashboard.id ? '#E3F2FD' : 'white'
                    }}
                    onMouseEnter={(e) => {
                      if (activeDashboardId !== dashboard.id) {
                        e.currentTarget.style.background = '#f5f5f5'
                        e.currentTarget.style.borderColor = '#2196F3'
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (activeDashboardId !== dashboard.id) {
                        e.currentTarget.style.background = 'white'
                        e.currentTarget.style.borderColor = '#ddd'
                      }
                    }}
                  >
                    <div style={{ fontWeight: 500, fontSize: '0.875rem', marginBottom: '0.25rem' }}>
                      {dashboard.name}
                      {activeDashboardId === dashboard.id && (
                        <span style={{ marginLeft: '0.5rem', color: '#2196F3', fontSize: '0.75rem' }}>(Most Recently Loaded)</span>
                      )}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: '#666' }}>
                      {dashboard.charts.length} chart{dashboard.charts.length !== 1 ? 's' : ''} · Created {new Date(dashboard.createdAt).toLocaleDateString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowLoadDashboardDialog(false)}
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

      {/* Manage Dashboards Dialog */}
      {showManageDashboardsDialog && (
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
          onClick={() => setShowManageDashboardsDialog(false)}
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
            <h3 style={{ margin: '0 0 1rem 0' }}>Manage Saved Dashboards</h3>
            {savedDashboards.length === 0 ? (
              <p style={{ color: '#999', textAlign: 'center', padding: '2rem' }}>
                No dashboards saved yet.
              </p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {savedDashboards.map((dashboard) => (
                  <div
                    key={dashboard.id}
                    style={{
                      border: '1px solid #ddd',
                      borderRadius: '6px',
                      padding: '0.75rem'
                    }}
                  >
                    {editingDashboardId === dashboard.id ? (
                      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.5rem' }}>
                        <input
                          type="text"
                          defaultValue={dashboard.name}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') renameDashboard(dashboard.id, e.currentTarget.value)
                            if (e.key === 'Escape') setEditingDashboardId(null)
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
                          onClick={(e) => {
                            const input = e.currentTarget.previousElementSibling as HTMLInputElement
                            renameDashboard(dashboard.id, input?.value || '')
                          }}
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
                          onClick={() => setEditingDashboardId(null)}
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
                        <strong style={{ fontSize: '0.875rem' }}>{dashboard.name}</strong>
                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                          <button
                            onClick={() => {
                              setEditingDashboardId(dashboard.id)
                              setEditingDashboardName(dashboard.name)
                            }}
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
                              if (window.confirm(`Delete dashboard "${dashboard.name}"?`)) {
                                deleteDashboard(dashboard.id)
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
                      {dashboard.charts.length} chart{dashboard.charts.length !== 1 ? 's' : ''} · Created {new Date(dashboard.createdAt).toLocaleDateString()}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'flex-end' }}>
              <button
                onClick={() => setShowManageDashboardsDialog(false)}
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

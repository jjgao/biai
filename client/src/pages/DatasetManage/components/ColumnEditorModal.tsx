import type { ColumnMetadata, ColumnMetadataUpdate } from '../types'

interface ColumnEditorModalProps {
  columns: ColumnMetadata[]
  loadingColumns: boolean
  onClose: () => void
  onUpdateColumn: (columnName: string, updates: ColumnMetadataUpdate) => void
}

export default function ColumnEditorModal({
  columns,
  loadingColumns,
  onClose,
  onUpdateColumn
}: ColumnEditorModalProps) {
  return (
    <div style={{
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
    }}>
      <div style={{
        background: 'white',
        borderRadius: '8px',
        padding: '2rem',
        maxWidth: '800px',
        maxHeight: '80vh',
        overflow: 'auto',
        width: '90%'
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
          <h3 style={{ margin: 0 }}>Manage Column Metadata</h3>
          <button
            onClick={onClose}
            style={{
              background: 'none',
              border: 'none',
              fontSize: '1.5rem',
              cursor: 'pointer',
              color: '#666'
            }}
          >
            ×
          </button>
        </div>

        {loadingColumns ? (
          <p>Loading columns...</p>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {columns.map((col) => (
              <div key={col.column_name} data-testid={`column-card-${col.column_name}`} style={{
                border: '1px solid #ddd',
                borderRadius: '4px',
                padding: '1rem',
                background: col.is_hidden ? '#f5f5f5' : 'white'
              }}>
                <div style={{ marginBottom: '0.5rem' }}>
                  <strong>{col.column_name}</strong>
                  {col.is_hidden && <span style={{ color: '#666', marginLeft: '0.5rem', fontSize: '0.875rem' }}>(Hidden)</span>}
                </div>
                <div style={{ display: 'grid', gap: '0.5rem' }}>
                  <div>
                    <label style={{ fontSize: '0.875rem', color: '#666' }}>Display Name:</label>
                    <input
                      type="text"
                      defaultValue={col.display_name}
                      onBlur={(e) => {
                        if (e.target.value !== col.display_name) {
                          onUpdateColumn(col.column_name, { displayName: e.target.value })
                        }
                      }}
                      style={{
                        width: '100%',
                        padding: '0.5rem',
                        border: '1px solid #ddd',
                        borderRadius: '4px',
                        marginTop: '0.25rem'
                      }}
                    />
                  </div>
                  <div>
                    <label style={{ fontSize: '0.875rem', color: '#666' }}>Description:</label>
                    <textarea
                      defaultValue={col.description}
                      onBlur={(e) => {
                        if (e.target.value !== col.description) {
                          onUpdateColumn(col.column_name, { description: e.target.value })
                        }
                      }}
                      style={{
                        width: '100%',
                        padding: '0.5rem',
                        border: '1px solid #ddd',
                        borderRadius: '4px',
                        marginTop: '0.25rem',
                        minHeight: '60px',
                        fontFamily: 'inherit'
                      }}
                    />
                  </div>
                  <div>
                    <label style={{ fontSize: '0.875rem', color: '#666' }}>Display Type:</label>
                    <select
                      value={col.display_type}
                      onChange={(e) => {
                        onUpdateColumn(col.column_name, { displayType: e.target.value })
                      }}
                      style={{
                        width: '100%',
                        padding: '0.5rem',
                        border: '1px solid #ddd',
                        borderRadius: '4px',
                        marginTop: '0.25rem'
                      }}
                    >
                      <option value="auto">Auto</option>
                      <option value="id">ID</option>
                      <option value="category">Category</option>
                      <option value="numeric">Numeric</option>
                      <option value="text">Text</option>
                      <option value="date">Date</option>
                      <option value="geographic">Geographic</option>
                      <option value="boolean">Boolean</option>
                    </select>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <label style={{ fontSize: '0.875rem', color: '#666', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                      <input
                        type="checkbox"
                        checked={col.is_hidden}
                        onChange={(e) => {
                          onUpdateColumn(col.column_name, { isHidden: e.target.checked })
                        }}
                      />
                      Hide this column
                    </label>
                    <span style={{ fontSize: '0.75rem', color: '#999', marginLeft: 'auto' }}>
                      Chart: {col.suggested_chart}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

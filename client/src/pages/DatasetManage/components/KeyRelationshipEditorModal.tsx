import type { Dataset, Relationship, ColumnMetadata, RelationshipFormState } from '../types'
import { resolveTableDisplayName } from '../utils'

interface KeyRelationshipEditorModalProps {
  dataset: Dataset
  keyEditorTableId: string | null
  keyEditorColumns: ColumnMetadata[]
  keyEditorLoading: boolean
  primaryKeySelection: string
  setPrimaryKeySelection: (key: string) => void
  primaryKeySaveDisabled: boolean
  primaryKeySaving: boolean
  tableRelationships: Relationship[]
  relationshipForm: RelationshipFormState
  setRelationshipForm: (form: RelationshipFormState | ((prev: RelationshipFormState) => RelationshipFormState)) => void
  referencedColumnsCache: Record<string, ColumnMetadata[]>
  referencedColumnsLoading: boolean
  relationshipSaving: boolean
  relationshipActionDisabled: boolean
  onClose: () => void
  onSavePrimaryKey: () => void
  onAddRelationship: () => void
  onDeleteRelationship: (rel: Relationship) => void
  onEnsureReferencedColumns: (tableId: string) => Promise<ColumnMetadata[]>
}

export default function KeyRelationshipEditorModal({
  dataset,
  keyEditorTableId,
  keyEditorColumns,
  keyEditorLoading,
  primaryKeySelection,
  setPrimaryKeySelection,
  primaryKeySaveDisabled,
  primaryKeySaving,
  tableRelationships,
  relationshipForm,
  setRelationshipForm,
  referencedColumnsCache,
  referencedColumnsLoading,
  relationshipSaving,
  relationshipActionDisabled,
  onClose,
  onSavePrimaryKey,
  onAddRelationship,
  onDeleteRelationship,
  onEnsureReferencedColumns
}: KeyRelationshipEditorModalProps) {
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
        maxWidth: '720px',
        maxHeight: '80vh',
        overflow: 'auto',
        width: '90%'
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
          <h3 style={{ margin: 0 }}>Manage Keys & Relationships</h3>
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

        {keyEditorLoading ? (
          <p>Loading metadata...</p>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
            <div>
              <h4 style={{ marginTop: 0 }}>Primary Key</h4>
              <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
                <select
                  value={primaryKeySelection}
                  onChange={(e) => setPrimaryKeySelection(e.target.value)}
                  style={{
                    flex: 1,
                    padding: '0.5rem',
                    border: '1px solid #ddd',
                    borderRadius: '4px'
                  }}
                >
                  <option value="">None</option>
                  {keyEditorColumns.map(col => (
                    <option key={col.column_name} value={col.column_name}>{col.column_name}</option>
                  ))}
                </select>
                <button
                  onClick={onSavePrimaryKey}
                  disabled={primaryKeySaveDisabled}
                  style={{
                    padding: '0.5rem 1rem',
                    background: primaryKeySaveDisabled ? '#ccc' : '#4CAF50',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: primaryKeySaveDisabled ? 'not-allowed' : 'pointer'
                  }}
                >
                  {primaryKeySaving ? 'Saving...' : 'Save'}
                </button>
              </div>
            </div>

            <div>
              <h4 style={{ marginTop: 0 }}>Relationships</h4>
              {tableRelationships.length === 0 ? (
                <p style={{ color: '#666' }}>No relationships defined.</p>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                  {tableRelationships.map(rel => (
                    <div
                      key={`${rel.foreignKey}->${rel.referencedTable}.${rel.referencedColumn}`}
                      style={{
                        padding: '0.75rem',
                        border: '1px solid #ddd',
                        borderRadius: '4px',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between'
                      }}
                    >
                      <div>
                        <strong>{rel.foreignKey}</strong> → {(rel.referencedTableDisplayName || resolveTableDisplayName(dataset, rel.referencedTable))}.{rel.referencedColumn}
                      </div>
                      <button
                        type="button"
                        onClick={() => onDeleteRelationship(rel)}
                        disabled={relationshipSaving}
                        style={{
                          background: '#f44336',
                          color: 'white',
                          border: 'none',
                          borderRadius: '4px',
                          padding: '0.35rem 0.75rem',
                          cursor: relationshipSaving ? 'not-allowed' : 'pointer',
                          fontSize: '0.8rem'
                        }}
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              )}

              <div style={{ marginTop: '1rem', padding: '1rem', background: '#f9f9f9', borderRadius: '6px', border: '1px solid #e0e0e0' }}>
                <h5 style={{ margin: '0 0 0.75rem 0' }}>Add Relationship</h5>
                <div style={{ display: 'grid', gap: '0.75rem', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
                  <div>
                    <label style={{ display: 'block', fontSize: '0.85rem', color: '#666', marginBottom: '0.25rem' }}>Column</label>
                    <select
                      value={relationshipForm.foreignKey}
                      onChange={(e) => setRelationshipForm(prev => ({ ...prev, foreignKey: e.target.value }))}
                      style={{ width: '100%', padding: '0.5rem', border: '1px solid #ddd', borderRadius: '4px' }}
                    >
                      <option value="">Select column...</option>
                      {keyEditorColumns.map(col => (
                        <option key={col.column_name} value={col.column_name}>{col.column_name}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: 'block', fontSize: '0.85rem', color: '#666', marginBottom: '0.25rem' }}>Referenced Table</label>
                    <select
                      value={relationshipForm.referencedTableId}
                      onChange={async (e) => {
                        const value = e.target.value
                        setRelationshipForm(prev => ({ ...prev, referencedTableId: value, referencedColumn: '' }))
                        if (value) {
                          await onEnsureReferencedColumns(value)
                        }
                      }}
                      style={{ width: '100%', padding: '0.5rem', border: '1px solid #ddd', borderRadius: '4px' }}
                    >
                      <option value="">Select table...</option>
                      {dataset?.tables
                        .filter(t => t.id !== keyEditorTableId)
                        .map(t => (
                          <option key={t.id} value={t.id}>{t.displayName}</option>
                        ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: 'block', fontSize: '0.85rem', color: '#666', marginBottom: '0.25rem' }}>Referenced Column</label>
                    <select
                      value={relationshipForm.referencedColumn}
                      onChange={(e) => setRelationshipForm(prev => ({ ...prev, referencedColumn: e.target.value }))}
                      disabled={!relationshipForm.referencedTableId || referencedColumnsLoading}
                      style={{ width: '100%', padding: '0.5rem', border: '1px solid #ddd', borderRadius: '4px' }}
                    >
                      <option value="">Select column...</option>
                      {(relationshipForm.referencedTableId ? referencedColumnsCache[relationshipForm.referencedTableId] || [] : []).map(col => (
                        <option key={col.column_name} value={col.column_name}>{col.column_name}</option>
                      ))}
                    </select>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'flex-end' }}>
                    <button
                      type="button"
                      onClick={onAddRelationship}
                      disabled={relationshipActionDisabled}
                      style={{
                        padding: '0.5rem 1rem',
                        background: relationshipActionDisabled ? '#ccc' : '#4CAF50',
                        color: 'white',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: relationshipActionDisabled ? 'not-allowed' : 'pointer'
                      }}
                    >
                      {relationshipSaving ? 'Saving...' : 'Add Relationship'}
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

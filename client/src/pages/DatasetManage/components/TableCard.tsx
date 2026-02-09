import type { Table } from '../types'
import { resolveTableDisplayName } from '../utils'
import type { Dataset } from '../types'

interface TableCardProps {
  table: Table
  dataset: Dataset
  isSelected: boolean
  isRenaming: boolean
  renamingTableName: string
  setRenamingTableName: (name: string) => void
  onStartRename: () => void
  onCancelRename: () => void
  onSaveRename: () => void
  onViewData: () => void
  onManageKeys: () => void
  onManageColumns: () => void
  onDelete: () => void
}

export default function TableCard({
  table,
  dataset,
  isSelected,
  isRenaming,
  renamingTableName,
  setRenamingTableName,
  onStartRename,
  onCancelRename,
  onSaveRename,
  onViewData,
  onManageKeys,
  onManageColumns,
  onDelete
}: TableCardProps) {
  return (
    <div
      data-testid={`table-card-${table.name}`}
      style={{
        background: 'white',
        padding: '1.5rem',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        border: isSelected ? '2px solid #2196F3' : '2px solid transparent'
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', marginBottom: '1rem' }}>
        <div>
          <h4 style={{ margin: '0 0 0.5rem 0', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            {isRenaming ? (
              <>
                <input
                  type="text"
                  data-testid="rename-table-input"
                  value={renamingTableName}
                  onChange={(e) => setRenamingTableName(e.target.value)}
                  style={{ fontSize: '0.9rem', padding: '0.25rem', borderRadius: '4px', border: '1px solid #ddd' }}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') onSaveRename()
                    if (e.key === 'Escape') onCancelRename()
                  }}
                  autoFocus
                  onClick={(e) => e.stopPropagation()}
                />
                <button
                  data-testid="save-rename-btn"
                  onClick={(e) => {
                    e.stopPropagation()
                    onSaveRename()
                  }}
                  style={{ padding: '0.25rem 0.5rem', background: '#4CAF50', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer', fontSize: '0.75rem' }}
                >
                  Save
                </button>
                <button
                  onClick={(e) => {
                    e.stopPropagation()
                    onCancelRename()
                  }}
                  style={{ padding: '0.25rem 0.5rem', background: '#ccc', color: 'black', border: 'none', borderRadius: '4px', cursor: 'pointer', fontSize: '0.75rem' }}
                >
                  Cancel
                </button>
              </>
            ) : (
              <>
                {table.displayName}
                <button
                  data-testid="rename-table-btn"
                  onClick={(e) => {
                    e.stopPropagation()
                    onStartRename()
                  }}
                  style={{ padding: '0.25rem', background: 'none', border: 'none', cursor: 'pointer', color: '#666', fontSize: '0.875rem' }}
                  title="Rename Table"
                >
                  ✏️
                </button>
              </>
            )}
          </h4>
          <div style={{ fontSize: '0.875rem', color: '#666' }}>
            <span>{table.filename}</span>
            <span style={{ margin: '0 1rem' }}>•</span>
            <span>{table.rowCount.toLocaleString()} rows</span>
            <span style={{ margin: '0 1rem' }}>•</span>
            <span>{table.columns.length} columns</span>
            {table.primaryKey && (
              <>
                <span style={{ margin: '0 1rem' }}>•</span>
                <span>PK: {table.primaryKey}</span>
              </>
            )}
          </div>
          {table.relationships && table.relationships.length > 0 && (
            <div style={{ fontSize: '0.875rem', color: '#666', marginTop: '0.5rem' }}>
              <strong>Relationships:</strong>
              {table.relationships.map((rel, i) => (
                <div key={`${rel.foreignKey}-${rel.referencedTable}-${rel.referencedColumn}-${i}`} style={{ marginLeft: '1rem', marginTop: '0.25rem' }}>
                  {rel.foreignKey} → {(rel.referencedTableDisplayName || resolveTableDisplayName(dataset, rel.referencedTable))}.{rel.referencedColumn} ({rel.type || 'many-to-one'})
                </div>
              ))}
            </div>
          )}
        </div>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button
            onClick={onViewData}
            style={{
              padding: '0.5rem 1rem',
              background: '#2196F3',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            View Data
          </button>
          <button
            onClick={onManageKeys}
            style={{
              padding: '0.5rem 1rem',
              background: '#673AB7',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            Manage Keys
          </button>
          <button
            onClick={onManageColumns}
            style={{
              padding: '0.5rem 1rem',
              background: '#FF9800',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            Manage Columns
          </button>
          <button
            onClick={onDelete}
            style={{
              padding: '0.5rem 1rem',
              background: '#f44336',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            Delete
          </button>
        </div>
      </div>

      <details>
        <summary style={{ cursor: 'pointer', color: '#666', fontSize: '0.875rem' }}>
          View columns ({table.columns.length})
        </summary>
        <div style={{ marginTop: '0.5rem', display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
          {table.columns.map((col, idx) => (
            <div key={idx} style={{ fontSize: '0.75rem', padding: '0.25rem', background: '#f5f5f5', borderRadius: '3px' }}>
              <strong>{col.name}</strong>: {col.type}{col.nullable ? '?' : ''}
            </div>
          ))}
        </div>
      </details>
    </div>
  )
}

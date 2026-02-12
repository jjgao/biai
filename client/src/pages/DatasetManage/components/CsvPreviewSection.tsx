import { useState } from 'react'
import type { Dataset, PreviewData } from '../types'

interface CsvPreviewSectionProps {
  dataset: Dataset
  previewData: PreviewData
  loadingPreview: boolean
  selectedPrimaryKey: string
  setSelectedPrimaryKey: (key: string) => void
  confirmedRelationships: any[]
  setConfirmedRelationships: (rels: any[]) => void
  selectedListColumns: Map<string, 'python' | 'json'>
  setSelectedListColumns: (cols: Map<string, 'python' | 'json'>) => void
}

export default function CsvPreviewSection({
  dataset,
  previewData,
  loadingPreview,
  selectedPrimaryKey,
  setSelectedPrimaryKey,
  confirmedRelationships,
  setConfirmedRelationships,
  selectedListColumns,
  setSelectedListColumns
}: CsvPreviewSectionProps) {
  // Controlled state for "Add FK manually" form (replacing document.getElementById)
  const [manualFkColumn, setManualFkColumn] = useState('')
  const [manualFkTable, setManualFkTable] = useState('')
  const [manualFkRefColumn, setManualFkRefColumn] = useState('')

  const selectedRefTable = dataset.tables.find(t => t.id === manualFkTable)

  if (loadingPreview) {
    return (
      <div style={{ padding: '1rem', background: '#f5f5f5', borderRadius: '4px', marginBottom: '1rem', textAlign: 'center' }}>
        Loading preview...
      </div>
    )
  }

  if (!previewData) return null

  return (
    <div style={{ marginBottom: '1rem', padding: '1.5rem', background: '#f9f9f9', borderRadius: '4px', border: '1px solid #ddd' }}>
      <h4 style={{ marginTop: 0, marginBottom: '1rem' }}>Data Preview</h4>

      <div style={{ marginBottom: '1rem', fontSize: '0.875rem', color: '#666' }}>
        <strong>Rows:</strong> {previewData.totalRows.toLocaleString()} | <strong>Columns:</strong> {previewData.columns.length}
      </div>

      {/* Primary Key Selector */}
      <div style={{ marginBottom: '1rem' }}>
        <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 'bold' }}>
          Primary Key (optional)
        </label>
        <select
          value={selectedPrimaryKey}
          onChange={(e) => setSelectedPrimaryKey(e.target.value)}
          style={{
            width: '100%',
            padding: '0.5rem',
            borderRadius: '4px',
            border: '1px solid #ddd'
          }}
        >
          <option value="">-- No Primary Key --</option>
          {previewData.columns.map((col) => (
            <option key={col.name} value={col.name}>
              {col.name} ({col.type})
            </option>
          ))}
        </select>
      </div>

      {/* Foreign Key Relationships */}
      <div style={{ marginBottom: '1rem' }}>
        <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 'bold' }}>
          Foreign Key Relationships
        </label>

        {/* Detected relationships */}
        {previewData.detectedRelationships && previewData.detectedRelationships.length > 0 && (
          <div style={{ marginBottom: '0.5rem' }}>
            <div style={{ fontSize: '0.875rem', color: '#666', marginBottom: '0.5rem' }}>
              Detected relationships (check to include):
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              {previewData.detectedRelationships.map((rel, idx) => (
                <div key={idx} style={{
                  padding: '0.75rem',
                  background: 'white',
                  borderRadius: '4px',
                  border: '1px solid #ddd'
                }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={confirmedRelationships.some(r => r.foreignKey === rel.foreignKey && r.referencedTable === rel.referencedTable)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setConfirmedRelationships([...confirmedRelationships, rel])
                        } else {
                          setConfirmedRelationships(confirmedRelationships.filter(r =>
                            !(r.foreignKey === rel.foreignKey && r.referencedTable === rel.referencedTable)
                          ))
                        }
                      }}
                    />
                    <div style={{ flex: 1 }}>
                      <div><strong>{rel.foreignKey}</strong> → {rel.referencedTable}.{rel.referencedColumn}</div>
                      <div style={{ fontSize: '0.75rem', color: '#666' }}>
                        Auto-detected by column name
                      </div>
                    </div>
                  </label>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Manually added relationships */}
        {confirmedRelationships.filter(r =>
          !previewData.detectedRelationships?.some((dr) =>
            dr.foreignKey === r.foreignKey && dr.referencedTable === r.referencedTable
          )
        ).length > 0 && (
          <div style={{ marginBottom: '0.5rem' }}>
            <div style={{ fontSize: '0.875rem', color: '#666', marginBottom: '0.5rem' }}>
              Manually added:
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              {confirmedRelationships.filter(r =>
                !previewData.detectedRelationships?.some((dr) =>
                  dr.foreignKey === r.foreignKey && dr.referencedTable === r.referencedTable
                )
              ).map((rel, idx) => (
                <div key={idx} style={{
                  padding: '0.75rem',
                  background: '#e3f2fd',
                  borderRadius: '4px',
                  border: '1px solid #2196F3',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between'
                }}>
                  <div>
                    <strong>{rel.foreignKey}</strong> → {rel.referencedTable}.{rel.referencedColumn}
                  </div>
                  <button
                    type="button"
                    onClick={() => {
                      setConfirmedRelationships(confirmedRelationships.filter(r =>
                        !(r.foreignKey === rel.foreignKey && r.referencedTable === rel.referencedTable)
                      ))
                    }}
                    style={{
                      background: '#f44336',
                      color: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      padding: '0.25rem 0.5rem',
                      cursor: 'pointer',
                      fontSize: '0.75rem'
                    }}
                  >
                    Remove
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Add manual relationship */}
        {dataset && dataset.tables.length > 0 && (
          <details style={{ marginTop: '0.5rem' }}>
            <summary style={{ cursor: 'pointer', fontSize: '0.875rem', color: '#2196F3' }}>
              + Add foreign key manually
            </summary>
            <div style={{ marginTop: '0.5rem', padding: '0.75rem', background: 'white', borderRadius: '4px', border: '1px solid #ddd' }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr auto', gap: '0.5rem', alignItems: 'end' }}>
                <div>
                  <label style={{ display: 'block', fontSize: '0.75rem', marginBottom: '0.25rem' }}>
                    Column
                  </label>
                  <select
                    value={manualFkColumn}
                    onChange={(e) => setManualFkColumn(e.target.value)}
                    style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd', fontSize: '0.875rem' }}
                  >
                    <option value="">Select...</option>
                    {previewData.columns.map((col) => (
                      <option key={col.name} value={col.name}>{col.name}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: '0.75rem', marginBottom: '0.25rem' }}>
                    References Table
                  </label>
                  <select
                    value={manualFkTable}
                    onChange={(e) => {
                      setManualFkTable(e.target.value)
                      setManualFkRefColumn('')
                    }}
                    style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd', fontSize: '0.875rem' }}
                  >
                    <option value="">Select...</option>
                    {dataset.tables.map((table) => (
                      <option key={table.id} value={table.id}>{table.displayName}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: '0.75rem', marginBottom: '0.25rem' }}>
                    References Column
                  </label>
                  <select
                    value={manualFkRefColumn}
                    onChange={(e) => setManualFkRefColumn(e.target.value)}
                    style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd', fontSize: '0.875rem' }}
                  >
                    <option value="">Select...</option>
                    {selectedRefTable?.columns.map(c => (
                      <option key={c.name} value={c.name}>{c.name}</option>
                    ))}
                  </select>
                </div>
                <button
                  type="button"
                  onClick={() => {
                    if (manualFkColumn && manualFkTable && manualFkRefColumn) {
                      const table = dataset.tables.find(t => t.id === manualFkTable)
                      if (table) {
                        const newRel = {
                          foreignKey: manualFkColumn,
                          referencedTable: table.name,
                          referencedTableId: table.id,
                          referencedColumn: manualFkRefColumn,
                          matchPercentage: 100,
                          sampleMatches: []
                        }
                        setConfirmedRelationships([...confirmedRelationships, newRel])
                        setManualFkColumn('')
                        setManualFkTable('')
                        setManualFkRefColumn('')
                      }
                    } else {
                      alert('Please select all fields')
                    }
                  }}
                  style={{
                    padding: '0.5rem 1rem',
                    background: '#4CAF50',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontSize: '0.875rem',
                    whiteSpace: 'nowrap'
                  }}
                >
                  Add
                </button>
              </div>
            </div>
          </details>
        )}
      </div>

      {/* List Columns Configuration */}
      {previewData.listSuggestions && previewData.listSuggestions.length > 0 && (
        <div style={{ marginBottom: '1rem' }}>
          <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 'bold' }}>
            List Columns (Arrays)
          </label>
          <div style={{ fontSize: '0.875rem', color: '#666', marginBottom: '0.5rem' }}>
            These columns appear to contain list values. Select which ones to parse as arrays:
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
            {previewData.listSuggestions.map((suggestion, idx) => {
              const isSelected = selectedListColumns.has(suggestion.columnName)
              const currentSyntax = selectedListColumns.get(suggestion.columnName) || suggestion.listSyntax

              return (
                <div key={idx} style={{
                  padding: '0.75rem',
                  background: isSelected ? '#e8f5e9' : 'white',
                  borderRadius: '4px',
                  border: isSelected ? '1px solid #4CAF50' : '1px solid #ddd'
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                    <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer', flex: 1 }}>
                      <input
                        type="checkbox"
                        checked={isSelected}
                        onChange={(e) => {
                          const newMap = new Map(selectedListColumns)
                          if (e.target.checked) {
                            newMap.set(suggestion.columnName, suggestion.listSyntax)
                          } else {
                            newMap.delete(suggestion.columnName)
                          }
                          setSelectedListColumns(newMap)
                        }}
                      />
                      <div style={{ flex: 1 }}>
                        <div style={{ fontWeight: 'bold' }}>{suggestion.columnName}</div>
                        <div style={{ fontSize: '0.75rem', color: '#666', marginTop: '0.25rem' }}>
                          Confidence: <span style={{
                            padding: '0.125rem 0.375rem',
                            borderRadius: '3px',
                            background: suggestion.confidence === 'high' ? '#4CAF50' : '#FF9800',
                            color: 'white',
                            fontWeight: 'bold'
                          }}>{suggestion.confidence}</span>
                          {' • '}
                          Avg {suggestion.avgItemCount} items/row
                          {' • '}
                          {suggestion.uniqueItemCount} unique values
                        </div>
                      </div>
                    </label>
                    {isSelected && (
                      <select
                        value={currentSyntax}
                        onChange={(e) => {
                          const newMap = new Map(selectedListColumns)
                          newMap.set(suggestion.columnName, e.target.value as 'python' | 'json')
                          setSelectedListColumns(newMap)
                        }}
                        style={{
                          padding: '0.375rem 0.5rem',
                          borderRadius: '4px',
                          border: '1px solid #ddd',
                          fontSize: '0.75rem'
                        }}
                      >
                        <option value="python">Python ['...']</option>
                        <option value="json">JSON ["..."]</option>
                      </select>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Sample Data */}
      <details open>
        <summary style={{ cursor: 'pointer', fontWeight: 'bold', marginBottom: '0.5rem' }}>
          Sample Data (first 10 rows)
        </summary>
        <div style={{ overflowX: 'auto', maxHeight: '300px', overflowY: 'auto', background: 'white', borderRadius: '4px' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem' }}>
            <thead style={{ position: 'sticky', top: 0, background: '#f5f5f5' }}>
              <tr>
                {previewData.columns.map((col) => (
                  <th key={col.name} style={{ padding: '0.5rem', textAlign: 'left', borderBottom: '2px solid #ddd' }}>
                    <div style={{ fontWeight: 'bold' }}>{col.name}</div>
                    <div style={{ fontSize: '0.7rem', color: '#666', fontWeight: 'normal' }}>
                      {col.type}{col.nullable ? '?' : ''}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {previewData.sampleRows.map((row, rowIdx) => (
                <tr key={rowIdx} style={{ borderBottom: '1px solid #eee' }}>
                  {row.map((val: any, colIdx: number) => (
                    <td key={colIdx} style={{ padding: '0.5rem' }}>
                      {val?.toString() || '-'}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </div>
  )
}

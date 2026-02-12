import { useState } from 'react'
import type { Dataset, SpreadsheetPreview, SheetImportConfig, Relationship, PotentialTarget } from '../types'

interface SpreadsheetSheetsConfigProps {
  dataset: Dataset
  spreadsheetPreview: SpreadsheetPreview
  sheetConfigs: SheetImportConfig[]
  setSheetConfigs: (configs: SheetImportConfig[]) => void
  removeSheetRelationship: (sheetIdx: number, relIdx: number) => void
  addSheetRelationship: (sheetIdx: number, rel: Relationship) => void
  getPotentialTargets: (currentSheetIdx: number) => PotentialTarget[]
}

export default function SpreadsheetSheetsConfig({
  dataset,
  spreadsheetPreview,
  sheetConfigs,
  setSheetConfigs,
  removeSheetRelationship,
  addSheetRelationship,
  getPotentialTargets
}: SpreadsheetSheetsConfigProps) {
  return (
    <div style={{ marginBottom: '1rem', padding: '1.5rem', background: '#f9f9f9', borderRadius: '4px', border: '1px solid #ddd' }}>
      <h4 style={{ marginTop: 0, marginBottom: '1rem' }}>Spreadsheet Sheets ({sheetConfigs.length})</h4>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {sheetConfigs.map((config, idx) => (
          <SheetConfigCard
            key={idx}
            idx={idx}
            config={config}
            dataset={dataset}
            spreadsheetPreview={spreadsheetPreview}
            sheetConfigs={sheetConfigs}
            setSheetConfigs={setSheetConfigs}
            removeSheetRelationship={removeSheetRelationship}
            addSheetRelationship={addSheetRelationship}
            getPotentialTargets={getPotentialTargets}
          />
        ))}
      </div>
    </div>
  )
}

interface SheetConfigCardProps {
  idx: number
  config: SheetImportConfig
  dataset: Dataset
  spreadsheetPreview: SpreadsheetPreview
  sheetConfigs: SheetImportConfig[]
  setSheetConfigs: (configs: SheetImportConfig[]) => void
  removeSheetRelationship: (sheetIdx: number, relIdx: number) => void
  addSheetRelationship: (sheetIdx: number, rel: Relationship) => void
  getPotentialTargets: (currentSheetIdx: number) => PotentialTarget[]
}

function SheetConfigCard({
  idx,
  config,
  dataset,
  spreadsheetPreview,
  sheetConfigs,
  setSheetConfigs,
  removeSheetRelationship,
  addSheetRelationship,
  getPotentialTargets
}: SheetConfigCardProps) {
  // Controlled state for the "Add Relationship" form (replacing document.getElementById)
  const [newRelForeignKey, setNewRelForeignKey] = useState('')
  const [newRelTable, setNewRelTable] = useState('')
  const [newRelColumn, setNewRelColumn] = useState('')

  const targets = getPotentialTargets(idx)
  const selectedTarget = targets.find(t => t.id === newRelTable)

  return (
    <div style={{ padding: '1rem', background: 'white', border: '1px solid #eee', borderRadius: '4px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '0.5rem' }}>
        <input
          type="checkbox"
          checked={config.selected}
          onChange={(e) => {
            const newConfigs = [...sheetConfigs]
            newConfigs[idx].selected = e.target.checked
            setSheetConfigs(newConfigs)
          }}
        />
        <strong style={{ flex: 1 }}>{config.sheetName}</strong>
        <span style={{ fontSize: '0.875rem', color: '#666' }}>
          {spreadsheetPreview.sheets[idx].rowCount} rows
        </span>
      </div>
      {config.selected && (
        <div style={{ marginLeft: '1.5rem', marginTop: '1rem' }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr 100px', gap: '1rem', marginBottom: '1rem' }}>
            <div>
              <label style={{ display: 'block', fontSize: '0.75rem', color: '#666' }}>Target</label>
              <select
                value={config.targetTableId || ''}
                onChange={(e) => {
                  const newConfigs = [...sheetConfigs]
                  const val = e.target.value

                  if (val.startsWith('PENDING:')) {
                    const pendingName = val.substring(8)
                    newConfigs[idx].targetTableId = val
                    newConfigs[idx].tableName = pendingName
                    newConfigs[idx].importMode = 'append'

                    const sourceSheet = sheetConfigs.find(s => s.tableName === pendingName && !s.targetTableId)
                    if (sourceSheet) {
                      newConfigs[idx].displayName = sourceSheet.displayName
                    }
                  } else {
                    newConfigs[idx].targetTableId = val
                    if (val) {
                      newConfigs[idx].importMode = 'append'
                      const existingTable = dataset?.tables.find(t => t.id === val)
                      if (existingTable) {
                        newConfigs[idx].displayName = existingTable.displayName
                      }
                    } else {
                      newConfigs[idx].importMode = 'append'
                      newConfigs[idx].displayName = config.sheetName
                    }
                  }
                  setSheetConfigs(newConfigs)
                }}
                style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd' }}
              >
                <option value="">New Table</option>
                {sheetConfigs.map((s, sIdx) => {
                  if (sIdx < idx && s.selected && s.tableName && !s.targetTableId) {
                    return <option key={`pending-${sIdx}`} value={`PENDING:${s.tableName}`}>{s.tableName} (New)</option>
                  }
                  return null
                })}
                {dataset?.tables.map(t => (
                  <option key={t.id} value={t.id}>{t.displayName}</option>
                ))}
              </select>
            </div>

            {config.targetTableId ? (
              <div>
                <label style={{ display: 'block', fontSize: '0.75rem', color: '#666' }}>Import Mode</label>
                <select
                  value={config.importMode}
                  onChange={(e) => {
                    const newConfigs = [...sheetConfigs]
                    newConfigs[idx].importMode = e.target.value as any
                    setSheetConfigs(newConfigs)
                  }}
                  style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd' }}
                >
                  <option value="append">Append</option>
                  <option value="replace">Replace</option>
                  <option value="upsert">Upsert</option>
                </select>
              </div>
            ) : (
              <div>
                <label style={{ display: 'block', fontSize: '0.75rem', color: '#666' }}>Table ID</label>
                <input
                  type="text"
                  value={config.tableName}
                  onChange={(e) => {
                    const newConfigs = [...sheetConfigs]
                    newConfigs[idx].tableName = e.target.value
                    setSheetConfigs(newConfigs)
                  }}
                  style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd' }}
                />
              </div>
            )}

            <div>
              <label style={{ display: 'block', fontSize: '0.75rem', color: '#666' }}>Display Name</label>
              <input
                type="text"
                value={config.displayName}
                onChange={(e) => {
                  const newConfigs = [...sheetConfigs]
                  newConfigs[idx].displayName = e.target.value
                  setSheetConfigs(newConfigs)
                }}
                disabled={!!config.targetTableId}
                style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd', background: config.targetTableId ? '#eee' : 'white' }}
              />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: '0.75rem', color: '#666' }}>Primary Key</label>
              <select
                value={config.primaryKey}
                onChange={(e) => {
                  const newConfigs = [...sheetConfigs]
                  newConfigs[idx].primaryKey = e.target.value
                  setSheetConfigs(newConfigs)
                }}
                disabled={!!config.targetTableId}
                style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd', background: config.targetTableId ? '#eee' : 'white' }}
              >
                <option value="">None</option>
                {spreadsheetPreview.sheets[idx].columns?.map((col: string) => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div>
              <label style={{ display: 'block', fontSize: '0.75rem', color: '#666' }}>Skip Rows</label>
              <input
                type="number"
                value={config.skipRows}
                min="0"
                onChange={(e) => {
                  const newConfigs = [...sheetConfigs]
                  newConfigs[idx].skipRows = parseInt(e.target.value, 10) || 0
                  setSheetConfigs(newConfigs)
                }}
                style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd' }}
              />
            </div>
          </div>

          {!config.targetTableId && (
            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', fontSize: '0.75rem', color: '#666', marginBottom: '0.25rem' }}>Relationships</label>

              {config.relationships && config.relationships.length > 0 && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem', marginBottom: '0.5rem' }}>
                  {config.relationships.map((rel, rIdx) => (
                    <div key={rIdx} style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem', background: '#e3f2fd', borderRadius: '3px', border: '1px solid #2196F3', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <span>{rel.foreignKey} → {rel.referencedTableDisplayName || rel.referencedTable}.{rel.referencedColumn}</span>
                      <button
                        type="button"
                        onClick={() => removeSheetRelationship(idx, rIdx)}
                        style={{ background: 'none', border: 'none', color: '#f44336', cursor: 'pointer', padding: 0, marginLeft: '0.5rem' }}
                      >
                        ×
                      </button>
                    </div>
                  ))}
                </div>
              )}

              <details>
                <summary style={{ fontSize: '0.75rem', cursor: 'pointer', color: '#2196F3' }}>+ Add Relationship</summary>
                <div style={{ marginTop: '0.5rem', padding: '0.5rem', background: '#f5f5f5', borderRadius: '4px', display: 'grid', gridTemplateColumns: '1fr 1fr 1fr auto', gap: '0.5rem', alignItems: 'end' }}>
                  <div>
                    <label style={{ display: 'block', fontSize: '0.7rem' }}>Col</label>
                    <select
                      value={newRelForeignKey}
                      onChange={(e) => setNewRelForeignKey(e.target.value)}
                      style={{ width: '100%', fontSize: '0.75rem', padding: '0.25rem' }}
                    >
                      <option value="">Select...</option>
                      {spreadsheetPreview.sheets[idx].columns?.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: 'block', fontSize: '0.7rem' }}>Ref Table</label>
                    <select
                      value={newRelTable}
                      onChange={(e) => {
                        setNewRelTable(e.target.value)
                        setNewRelColumn('')
                      }}
                      style={{ width: '100%', fontSize: '0.75rem', padding: '0.25rem' }}
                    >
                      <option value="">Select...</option>
                      {targets.map(t => (
                        <option key={t.id} value={t.id}>{t.displayName}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: 'block', fontSize: '0.7rem' }}>Ref Col</label>
                    <select
                      value={newRelColumn}
                      onChange={(e) => setNewRelColumn(e.target.value)}
                      style={{ width: '100%', fontSize: '0.75rem', padding: '0.25rem' }}
                    >
                      <option value="">Select...</option>
                      {selectedTarget?.columns.map(c => (
                        <option key={c} value={c}>{c}</option>
                      ))}
                    </select>
                  </div>
                  <button
                    type="button"
                    style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem', background: '#4CAF50', color: 'white', border: 'none', borderRadius: '3px', cursor: 'pointer' }}
                    onClick={() => {
                      if (newRelForeignKey && newRelTable && newRelColumn) {
                        addSheetRelationship(idx, {
                          foreignKey: newRelForeignKey,
                          referencedTable: newRelTable,
                          referencedColumn: newRelColumn,
                          type: 'many-to-one',
                          referencedTableDisplayName: selectedTarget?.displayName || newRelTable
                        })
                        setNewRelForeignKey('')
                        setNewRelTable('')
                        setNewRelColumn('')
                      }
                    }}
                  >
                    Add
                  </button>
                </div>
              </details>
            </div>
          )}

          {spreadsheetPreview.sheets[idx].preview && (
            <details>
              <summary style={{ fontSize: '0.75rem', cursor: 'pointer', color: '#2196F3' }}>
                Preview Data
              </summary>
              <div style={{
                marginTop: '0.5rem',
                overflowX: 'auto',
                maxHeight: '200px',
                border: '1px solid #eee',
                borderRadius: '4px'
              }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.7rem' }}>
                  <thead style={{ position: 'sticky', top: 0, background: '#f5f5f5' }}>
                    <tr>
                      {spreadsheetPreview.sheets[idx].columns?.map((col, cIdx) => (
                        <th key={cIdx} style={{ padding: '0.25rem 0.5rem', textAlign: 'left', borderBottom: '1px solid #ddd' }}>
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {spreadsheetPreview.sheets[idx].preview?.slice(1, 6).map((row, rIdx) => (
                      <tr key={rIdx} style={{ borderBottom: '1px solid #eee' }}>
                        {row.map((cell: any, cIdx: number) => (
                          <td key={cIdx} style={{ padding: '0.25rem 0.5rem' }}>
                            {String(cell ?? '')}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          )}
        </div>
      )}
    </div>
  )
}

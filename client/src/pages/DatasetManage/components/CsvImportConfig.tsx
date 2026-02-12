import type { Dataset } from '../types'

interface CsvImportConfigProps {
  dataset: Dataset
  importTarget: 'new' | 'existing'
  setImportTarget: (target: 'new' | 'existing') => void
  targetTableId: string
  setTargetTableId: (id: string) => void
  importModeType: 'append' | 'replace' | 'upsert'
  setImportModeType: (mode: 'append' | 'replace' | 'upsert') => void
  tableName: string
  setTableName: (name: string) => void
  displayName: string
  setDisplayName: (name: string) => void
  skipRows: string
  setSkipRows: (rows: string) => void
  delimiter: string
  setDelimiter: (d: string) => void
  wasDelimiterDetected: boolean
  setWasDelimiterDetected: (detected: boolean) => void
  detectedDelimiterName: string
  primaryKey: string
  setPrimaryKey: (key: string) => void
  isSpreadsheet: boolean
}

export default function CsvImportConfig({
  dataset,
  importTarget,
  setImportTarget,
  targetTableId,
  setTargetTableId,
  importModeType,
  setImportModeType,
  tableName,
  setTableName,
  displayName,
  setDisplayName,
  skipRows,
  setSkipRows,
  delimiter,
  setDelimiter,
  wasDelimiterDetected,
  setWasDelimiterDetected,
  detectedDelimiterName,
  primaryKey,
  setPrimaryKey,
  isSpreadsheet
}: CsvImportConfigProps) {
  if (isSpreadsheet) return null

  return (
    <>
      <div style={{ marginBottom: '1rem', padding: '1rem', background: '#f5f5f5', borderRadius: '4px' }}>
        <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 'bold' }}>Target</label>
        <div style={{ display: 'flex', gap: '1.5rem', marginBottom: '1rem' }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
            <input
              type="radio"
              name="importTarget"
              value="new"
              checked={importTarget === 'new'}
              onChange={() => setImportTarget('new')}
            />
            Create New Table
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
            <input
              type="radio"
              name="importTarget"
              value="existing"
              checked={importTarget === 'existing'}
              onChange={() => setImportTarget('existing')}
              disabled={!dataset?.tables || dataset.tables.length === 0}
            />
            Import to Existing Table
          </label>
        </div>

        {importTarget === 'existing' && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
            <div>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Select Table</label>
              <select
                value={targetTableId}
                onChange={(e) => setTargetTableId(e.target.value)}
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
                required
              >
                <option value="">-- Select Table --</option>
                {dataset?.tables.map(t => (
                  <option key={t.id} value={t.id}>{t.displayName} ({t.rowCount.toLocaleString()} rows)</option>
                ))}
              </select>
            </div>
            <div>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Import Mode</label>
              <select
                value={importModeType}
                onChange={(e) => setImportModeType(e.target.value as any)}
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              >
                <option value="append">Append (Add rows)</option>
                <option value="replace">Replace (Overwrite table)</option>
                <option value="upsert">Upsert (Update by PK)</option>
              </select>
              <div style={{ fontSize: '0.75rem', color: '#666', marginTop: '0.25rem' }}>
                {importModeType === 'append' && 'Adds new rows. May create duplicates if no PK.'}
                {importModeType === 'replace' && 'Deletes ALL existing rows first.'}
                {importModeType === 'upsert' && 'Updates rows with matching Primary Key.'}
              </div>
            </div>
          </div>
        )}
      </div>

      {importTarget === 'new' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
          <div>
            <label style={{ display: 'block', marginBottom: '0.5rem' }}>Table Name (identifier) *</label>
            <input
              type="text"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              required={!isSpreadsheet && importTarget === 'new'}
              placeholder=""
              style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: '0.5rem' }}>Display Name</label>
            <input
              type="text"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder=""
              style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
            />
          </div>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
        <div>
          <label style={{ display: 'block', marginBottom: '0.5rem' }}>Skip Rows</label>
          <input
            type="number"
            value={skipRows}
            onChange={(e) => setSkipRows(e.target.value)}
            min="0"
            style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
          />
        </div>
        <div>
          <label style={{ display: 'block', marginBottom: '0.5rem' }}>Delimiter</label>
          <select
            value={delimiter}
            onChange={(e) => {
              setDelimiter(e.target.value)
              if (wasDelimiterDetected) {
                setWasDelimiterDetected(false)
              }
            }}
            style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
          >
            <option value="\t">Tab</option>
            <option value=",">Comma</option>
            <option value=";">Semicolon</option>
            <option value="|">Pipe</option>
          </select>
          {wasDelimiterDetected && (
            <div
              style={{
                marginTop: '0.25rem',
                fontSize: '0.75rem',
                color: '#4CAF50',
                display: 'flex',
                alignItems: 'center',
                gap: '0.25rem'
              }}
            >
              <span>✓</span>
              <span>Auto-detected: {detectedDelimiterName}</span>
            </div>
          )}
        </div>
        <div>
          <label style={{ display: 'block', marginBottom: '0.5rem' }}>Primary Key (optional)</label>
          <input
            type="text"
            value={primaryKey}
            onChange={(e) => setPrimaryKey(e.target.value)}
            placeholder=""
            style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
          />
        </div>
      </div>
    </>
  )
}

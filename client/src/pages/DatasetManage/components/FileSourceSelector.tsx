interface FileSourceSelectorProps {
  importMode: 'file' | 'url'
  setImportMode: (mode: 'file' | 'url') => void
  fileUrl: string
  loadingPreview: boolean
  onFileSelect: (e: React.ChangeEvent<HTMLInputElement>) => void
  onUrlChange: (e: React.ChangeEvent<HTMLInputElement>) => void
  onUrlLoad: () => void
}

export default function FileSourceSelector({
  importMode,
  setImportMode,
  fileUrl,
  loadingPreview,
  onFileSelect,
  onUrlChange,
  onUrlLoad
}: FileSourceSelectorProps) {
  return (
    <>
      <div style={{ marginBottom: '1rem' }}>
        <label style={{ display: 'block', marginBottom: '0.5rem' }}>Import Method</label>
        <div style={{ display: 'flex', gap: '1rem' }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <input
              type="radio"
              name="importMode"
              value="file"
              checked={importMode === 'file'}
              onChange={(e) => setImportMode(e.target.value as 'file' | 'url')}
            />
            Upload File
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <input
              type="radio"
              name="importMode"
              value="url"
              checked={importMode === 'url'}
              onChange={(e) => setImportMode(e.target.value as 'file' | 'url')}
            />
            From URL
          </label>
        </div>
      </div>

      {importMode === 'file' ? (
        <div style={{ marginBottom: '1rem' }}>
          <label style={{ display: 'block', marginBottom: '0.5rem' }}>File</label>
          <input type="file" accept=".csv,.txt,.tsv,.xlsx,.xls,.ods" onChange={onFileSelect} required />
        </div>
      ) : (
        <div style={{ marginBottom: '1rem' }}>
          <label style={{ display: 'block', marginBottom: '0.5rem' }}>File URL</label>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <input
              type="url"
              value={fileUrl}
              onChange={onUrlChange}
              onBlur={() => {
                if (fileUrl) onUrlLoad()
              }}
              placeholder=""
              style={{ flex: 1, padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              required
            />
            <button
              type="button"
              onClick={onUrlLoad}
              disabled={!fileUrl || loadingPreview}
              style={{
                padding: '0.5rem 1rem',
                background: loadingPreview ? '#ccc' : '#2196F3',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: loadingPreview ? 'not-allowed' : 'pointer',
                whiteSpace: 'nowrap'
              }}
            >
              {loadingPreview ? 'Loading...' : 'Load'}
            </button>
          </div>
          <small style={{ color: '#666', fontSize: '0.875rem' }}>
            Provide a direct URL to a CSV, TSV, TXT, or Spreadsheet file and click Load
          </small>
        </div>
      )}
    </>
  )
}

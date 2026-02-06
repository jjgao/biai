import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import SafeHtml from '../components/SafeHtml'
import api from '../services/api'

interface DatasetTable {
  id: string
  name: string
  displayName: string
  rowCount: number
}

interface ConnectionInfo {
  host: string
  port?: number
  protocol?: 'http' | 'https'
  username?: string
}

interface Dataset {
  id: string
  name: string
  database_name: string
  database_type: 'created' | 'connected'
  description: string
  tags?: string[]
  source?: string
  citation?: string
  references?: string[]
  tableCount: number
  tables: DatasetTable[]
  connectionInfo?: ConnectionInfo | null
  createdAt: string
  updatedAt: string
}

interface DatabaseInfo {
  name: string
}

function Datasets() {
  const navigate = useNavigate()
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [loading, setLoading] = useState(true)
  const [showCreate, setShowCreate] = useState(false)
  const [showImport, setShowImport] = useState(false)
  const [datasetName, setDatasetName] = useState('')
  const [description, setDescription] = useState('')
  const [creating, setCreating] = useState(false)
  const [importing, setImporting] = useState(false)

  // Import from directory state
  const [showDirectoryImport, setShowDirectoryImport] = useState(false)
  const [directoryPath, setDirectoryPath] = useState('')
  const [importingDirectory, setImportingDirectory] = useState(false)
  const [directoryImportError, setDirectoryImportError] = useState<string | null>(null)

  // Import database state
  const [availableDatabases, setAvailableDatabases] = useState<DatabaseInfo[]>([])
  const [selectedDatabase, setSelectedDatabase] = useState('')
  const [importDisplayName, setImportDisplayName] = useState('')
  const [importDescription, setImportDescription] = useState('')
  const [connectionHost, setConnectionHost] = useState('localhost')
  const [connectionPort, setConnectionPort] = useState('8123')
  const [connectionSecure, setConnectionSecure] = useState(false)
  const [connectionUsername, setConnectionUsername] = useState('')
  const [connectionPassword, setConnectionPassword] = useState('')
  const [loadingDatabases, setLoadingDatabases] = useState(false)
  const [databaseLoadError, setDatabaseLoadError] = useState<string | null>(null)

  useEffect(() => {
    loadDatasets()
  }, [])

  const loadDatasets = async () => {
    try {
      setLoading(true)
      const response = await api.get('/datasets')
      setDatasets(response.data.datasets)
    } catch (error) {
      console.error('Failed to load datasets:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!datasetName) return

    try {
      setCreating(true)
      const response = await api.post('/datasets', {
        name: datasetName,
        description: description
      })
      setShowCreate(false)
      setDatasetName('')
      setDescription('')

      // Navigate to the new dataset to add tables
      navigate(`/datasets/${response.data.dataset.id}/manage`)
    } catch (error: any) {
      console.error('Create failed:', error)
      alert('Create failed: ' + (error.response?.data?.message || error.message))
    } finally {
      setCreating(false)
    }
  }

  const loadAvailableDatabases = async () => {
    if (!connectionHost.trim()) {
      alert('Host is required to load databases')
      return
    }

    try {
      setLoadingDatabases(true)
      setDatabaseLoadError(null)

      const response = await api.post('/databases/list', {
        host: connectionHost.trim(),
        port: connectionPort ? Number(connectionPort) : undefined,
        secure: connectionSecure,
        username: connectionUsername || undefined,
        password: connectionPassword || undefined
      })

      const databases: DatabaseInfo[] = response.data?.databases || []
      setAvailableDatabases(databases)
      if (databases.length === 1 && !selectedDatabase) {
        setSelectedDatabase(databases[0].name)
        if (!importDisplayName) {
          setImportDisplayName(databases[0].name)
        }
      }
    } catch (error: any) {
      console.error('Failed to load databases:', error)
      const message = error.response?.data?.message || error.message || 'Failed to load databases'
      setDatabaseLoadError(message)
      setAvailableDatabases([])
    } finally {
      setLoadingDatabases(false)
    }
  }

  const handleShowImport = () => {
    if (showImport) {
      setShowImport(false)
      return
    }
    setShowImport(true)
    setShowCreate(false)
    setShowDirectoryImport(false)
    setAvailableDatabases([])
    setSelectedDatabase('')
    setImportDisplayName('')
    setImportDescription('')
    setConnectionHost('localhost')
    setConnectionPort('8123')
    setConnectionSecure(false)
    setConnectionUsername('')
    setConnectionPassword('')
    setDatabaseLoadError(null)
  }

  const handleImport = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!selectedDatabase || !importDisplayName || !connectionHost.trim()) {
      alert('Host, database, and display name are required')
      return
    }

    try {
      setImporting(true)
      await api.post('/datasets/connect', {
        databaseName: selectedDatabase,
        displayName: importDisplayName,
        description: importDescription,
        host: connectionHost.trim(),
        port: connectionPort ? Number(connectionPort) : undefined,
        secure: connectionSecure,
        username: connectionUsername || undefined,
        password: connectionPassword || undefined
      })
      setShowImport(false)
      setSelectedDatabase('')
      setImportDisplayName('')
      setImportDescription('')
      setConnectionHost('')
      setConnectionPort('8123')
      setConnectionSecure(false)
      setConnectionUsername('')
      setConnectionPassword('')
      setAvailableDatabases([])

      // Reload datasets list
      loadDatasets()
    } catch (error: any) {
      console.error('Import failed:', error)
      alert('Import failed: ' + (error.response?.data?.message || error.message))
    } finally {
      setImporting(false)
    }
  }

  const handleShowDirectoryImport = () => {
    if (showDirectoryImport) {
      setShowDirectoryImport(false)
      return
    }
    setShowDirectoryImport(true)
    setShowCreate(false)
    setShowImport(false)
    setDirectoryPath('')
    setDirectoryImportError(null)
  }

  const handleDirectoryImport = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!directoryPath.trim()) return

    try {
      setImportingDirectory(true)
      setDirectoryImportError(null)
      const response = await api.post('/datasets/import-from-path', {
        path: directoryPath.trim()
      })
      setShowDirectoryImport(false)
      setDirectoryPath('')
      navigate(`/datasets/${response.data.datasetId}`)
    } catch (error: any) {
      console.error('Directory import failed:', error)
      setDirectoryImportError(error.response?.data?.message || error.message || 'Import failed')
    } finally {
      setImportingDirectory(false)
    }
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '2rem' }}>
        <h2>Datasets</h2>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button
            onClick={() => {
              setShowCreate(!showCreate)
              setShowImport(false)
              setShowDirectoryImport(false)
            }}
            style={{
              padding: '0.5rem 1rem',
              background: showCreate ? '#ccc' : '#4CAF50',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            {showCreate ? 'Cancel' : '+ Create Dataset'}
          </button>
          <button
            onClick={handleShowDirectoryImport}
            style={{
              padding: '0.5rem 1rem',
              background: showDirectoryImport ? '#ccc' : '#FF9800',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            {showDirectoryImport ? 'Cancel' : 'Import from Directory'}
          </button>
          <button
            onClick={handleShowImport}
            style={{
              padding: '0.5rem 1rem',
              background: showImport ? '#ccc' : '#2196F3',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            {showImport ? 'Cancel' : 'Import Database'}
          </button>
        </div>
      </div>

      {showCreate && (
        <div style={{
          background: 'white',
          padding: '2rem',
          borderRadius: '8px',
          marginBottom: '2rem',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
        }}>
          <h3 style={{ marginTop: 0 }}>Create New Dataset</h3>
          <form onSubmit={handleCreate}>
            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Dataset Name *</label>
              <input
                type="text"
                value={datasetName}
                onChange={(e) => setDatasetName(e.target.value)}
                required
                placeholder="e.g., TCGA GBM Study"
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              />
            </div>

            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Description</label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                rows={3}
                placeholder="Describe this dataset..."
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              />
            </div>

            <button
              type="submit"
              disabled={creating || !datasetName}
              style={{
                padding: '0.75rem 1.5rem',
                background: creating ? '#ccc' : '#4CAF50',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: creating ? 'not-allowed' : 'pointer'
              }}
            >
              {creating ? 'Creating...' : 'Create Dataset'}
            </button>
          </form>
        </div>
      )}

      {showDirectoryImport && (
        <div style={{
          background: 'white',
          padding: '2rem',
          borderRadius: '8px',
          marginBottom: '2rem',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
        }}>
          <h3 style={{ marginTop: 0 }}>Import from Directory</h3>
          <p style={{ color: '#666', marginBottom: '1.5rem' }}>
            Import a dataset from a local directory containing .meta configuration files and data files
          </p>
          <form onSubmit={handleDirectoryImport}>
            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Directory Path *</label>
              <input
                type="text"
                value={directoryPath}
                onChange={(e) => {
                  setDirectoryPath(e.target.value)
                  setDirectoryImportError(null)
                }}
                required
                placeholder="e.g., example_data/gbm_tcga_pan_can_atlas_2018"
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              />
            </div>

            {directoryImportError && (
              <div style={{ marginBottom: '1rem', color: '#d32f2f', fontSize: '0.875rem' }}>
                {directoryImportError}
              </div>
            )}

            <button
              type="submit"
              disabled={importingDirectory || !directoryPath.trim()}
              style={{
                padding: '0.75rem 1.5rem',
                background: (importingDirectory || !directoryPath.trim()) ? '#ccc' : '#FF9800',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: (importingDirectory || !directoryPath.trim()) ? 'not-allowed' : 'pointer'
              }}
            >
              {importingDirectory ? 'Importing...' : 'Import Dataset'}
            </button>
          </form>
        </div>
      )}

      {showImport && (
        <div style={{
          background: 'white',
          padding: '2rem',
          borderRadius: '8px',
          marginBottom: '2rem',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
        }}>
          <h3 style={{ marginTop: 0 }}>Import Existing Database</h3>
          <p style={{ color: '#666', marginBottom: '1.5rem' }}>
            Register an existing ClickHouse database to explore its data
          </p>
          <form onSubmit={handleImport}>
            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Host *</label>
              <input
                type="text"
                value={connectionHost}
                onChange={(e) => setConnectionHost(e.target.value)}
                required
                placeholder="clickhouse.mycompany.com"
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              />
            </div>

            <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
              <div style={{ flex: 1 }}>
                <label style={{ display: 'block', marginBottom: '0.5rem' }}>Port</label>
                <input
                  type="number"
                  value={connectionPort}
                  onChange={(e) => setConnectionPort(e.target.value)}
                  placeholder={connectionSecure ? '8443' : '8123'}
                  style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
                />
              </div>
              <div style={{ display: 'flex', alignItems: 'flex-end' }}>
                <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                  <input
                    type="checkbox"
                    checked={connectionSecure}
                    onChange={(e) => setConnectionSecure(e.target.checked)}
                  />
                  Use HTTPS
                </label>
              </div>
            </div>

            <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
              <div style={{ flex: 1 }}>
                <label style={{ display: 'block', marginBottom: '0.5rem' }}>Username</label>
                <input
                  type="text"
                  value={connectionUsername}
                  onChange={(e) => setConnectionUsername(e.target.value)}
                  placeholder="default"
                  style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
                />
              </div>
              <div style={{ flex: 1 }}>
                <label style={{ display: 'block', marginBottom: '0.5rem' }}>Password</label>
                <input
                  type="password"
                  value={connectionPassword}
                  onChange={(e) => setConnectionPassword(e.target.value)}
                  placeholder="Optional"
                  style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
                />
              </div>
            </div>

            <div style={{ marginBottom: '1rem' }}>
              <button
                type="button"
                onClick={loadAvailableDatabases}
                disabled={loadingDatabases || !connectionHost.trim()}
                style={{
                  padding: '0.5rem 1rem',
                  background: (loadingDatabases || !connectionHost.trim()) ? '#ccc' : '#2196F3',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: (loadingDatabases || !connectionHost.trim()) ? 'not-allowed' : 'pointer'
                }}
              >
                {loadingDatabases ? 'Loading...' : 'Load Databases'}
              </button>
            </div>

            {databaseLoadError && (
              <div style={{ marginBottom: '1rem', color: '#d32f2f', fontSize: '0.875rem' }}>
                {databaseLoadError}
              </div>
            )}

            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Select Database *</label>
              <select
                value={selectedDatabase}
                onChange={(e) => {
                  setSelectedDatabase(e.target.value)
                  if (!importDisplayName) {
                    setImportDisplayName(e.target.value)
                  }
                }}
                required
                disabled={availableDatabases.length === 0}
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              >
                <option value="">Choose a database...</option>
                {availableDatabases.map((db) => (
                  <option key={db.name} value={db.name}>{db.name}</option>
                ))}
              </select>
            </div>

            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Display Name *</label>
              <input
                type="text"
                value={importDisplayName}
                onChange={(e) => setImportDisplayName(e.target.value)}
                required
                placeholder="e.g., Analytics Database"
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              />
            </div>

            <div style={{ marginBottom: '1rem' }}>
              <label style={{ display: 'block', marginBottom: '0.5rem' }}>Description</label>
              <textarea
                value={importDescription}
                onChange={(e) => setImportDescription(e.target.value)}
                rows={3}
                placeholder="Describe this database..."
                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
              />
            </div>

            <button
              type="submit"
              disabled={importing || !selectedDatabase || !importDisplayName || !connectionHost.trim()}
              style={{
                padding: '0.75rem 1.5rem',
                background: (importing || !selectedDatabase || !importDisplayName || !connectionHost.trim()) ? '#ccc' : '#2196F3',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: (importing || !selectedDatabase || !importDisplayName || !connectionHost.trim()) ? 'not-allowed' : 'pointer'
              }}
            >
              {importing ? 'Importing...' : 'Import Database'}
            </button>
          </form>
        </div>
      )}

      {loading ? (
        <p>Loading datasets...</p>
      ) : datasets.length === 0 ? (
        <div style={{
          background: 'white',
          padding: '3rem',
          borderRadius: '8px',
          textAlign: 'center',
          color: '#666'
        }}>
          <p>No datasets yet. Create your first dataset to get started!</p>
        </div>
      ) : (
        <div style={{ display: 'grid', gap: '1rem' }}>
          {datasets.map((dataset) => (
            <div
              key={dataset.id}
              style={{
                background: 'white',
                padding: '1.5rem',
                borderRadius: '8px',
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', marginBottom: '1rem' }}>
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                    <h3 style={{ margin: 0 }}>{dataset.name}</h3>
                    {dataset.database_type === 'connected' && (
                      <span style={{
                        padding: '0.25rem 0.5rem',
                        background: '#e3f2fd',
                        color: '#1976d2',
                        borderRadius: '4px',
                        fontSize: '0.75rem',
                        fontWeight: 500
                      }}>
                        Connected
                      </span>
                    )}
                  </div>
                  {dataset.description && (
                    <SafeHtml
                      html={dataset.description}
                      style={{ margin: '0 0 0.5rem 0', color: '#666', display: 'block' }}
                    />
                  )}
                  {dataset.database_type === 'connected' && dataset.connectionInfo && (
                    <div style={{ fontSize: '0.8rem', color: '#555', marginBottom: '0.5rem' }}>
                      <strong>Host:</strong> {dataset.connectionInfo.host}
                      {dataset.connectionInfo.port ? `:${dataset.connectionInfo.port}` : ''}
                      {dataset.connectionInfo.protocol ? ` (${dataset.connectionInfo.protocol.toUpperCase()})` : ''}
                      {dataset.connectionInfo.username ? ` • User: ${dataset.connectionInfo.username}` : ''}
                    </div>
                  )}
                  {dataset.tags && dataset.tags.length > 0 && (
                    <div style={{ margin: '0.5rem 0', display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                      {dataset.tags.map((tag, i) => (
                        <span
                          key={i}
                          style={{
                            padding: '0.25rem 0.5rem',
                            background: '#e3f2fd',
                            color: '#1976d2',
                            borderRadius: '4px',
                            fontSize: '0.75rem',
                            fontWeight: 500
                          }}
                        >
                          {tag}
                        </span>
                      ))}
                    </div>
                  )}
                  {dataset.source && (
                    <div style={{ fontSize: '0.875rem', color: '#666', margin: '0.5rem 0' }}>
                      <strong>Source:</strong> {dataset.source}
                    </div>
                  )}
                  {dataset.citation && (
                    <div style={{ fontSize: '0.875rem', color: '#666', margin: '0.5rem 0' }}>
                      <strong>Citation:</strong> {dataset.citation}
                    </div>
                  )}
                  <div style={{ fontSize: '0.875rem', color: '#999' }}>
                    <span>{dataset.tableCount} table{dataset.tableCount !== 1 ? 's' : ''}</span>
                    <span style={{ margin: '0 1rem' }}>•</span>
                    <span>Updated {new Date(dataset.updatedAt).toLocaleDateString()}</span>
                  </div>
                </div>
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  <button
                    onClick={() => navigate(`/datasets/${dataset.id}`)}
                    style={{
                      padding: '0.5rem 1rem',
                      background: '#2196F3',
                      color: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      cursor: 'pointer'
                    }}
                  >
                    📊 Explore Data
                  </button>
                  <button
                    onClick={() => navigate(`/datasets/${dataset.id}/manage`)}
                    style={{
                      padding: '0.5rem',
                      background: '#757575',
                      color: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      fontSize: '1.2rem',
                      lineHeight: '1',
                      width: '32px',
                      height: '32px',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center'
                    }}
                    title="Manage dataset"
                  >
                    ✎
                  </button>
                </div>
              </div>

              {dataset.tables && dataset.tables.length > 0 && (
                <div style={{ marginTop: '1rem', paddingTop: '1rem', borderTop: '1px solid #eee' }}>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
                    {dataset.tables.map((table) => (
                      <div
                        key={table.id}
                        style={{
                          padding: '0.5rem',
                          background: '#f5f5f5',
                          borderRadius: '4px',
                          fontSize: '0.875rem'
                        }}
                      >
                        <div style={{ fontWeight: 500 }}>{table.displayName}</div>
                        <div style={{ color: '#666', fontSize: '0.75rem' }}>
                          {table.rowCount.toLocaleString()} rows
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default Datasets

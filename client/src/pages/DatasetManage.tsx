import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import SafeHtml from '../components/SafeHtml'
import TemporalConfigPanel from '../components/TemporalConfigPanel'
import api from '../services/api'

interface Column {
  name: string
  type: string
  nullable: boolean
}

interface Relationship {
  foreignKey: string
  referencedTable: string
  referencedColumn: string
  type?: string
  referencedTableDisplayName?: string
}

interface Table {
  id: string
  name: string
  displayName: string
  filename: string
  rowCount: number
  columns: Column[]
  primaryKey?: string
  relationships?: Relationship[]
  customMetadata?: string
  createdAt: string
}

interface Dataset {
  id: string
  name: string
  description: string
  tags?: string[]
  source?: string
  citation?: string
  references?: string[]
  customMetadata?: string
  tables: Table[]
  createdAt: string
  updatedAt: string
}

interface ColumnMetadata {
  column_name: string
  column_type: string
  display_name: string
  description: string
  is_hidden: boolean
  display_type: string
  suggested_chart: string
  temporal_role?: 'none' | 'start_date' | 'stop_date' | 'duration'
  temporal_paired_column?: string
  temporal_unit?: 'days' | 'months' | 'years'
}

interface ColumnMetadataUpdate {
  displayName?: string
  description?: string
  isHidden?: boolean
  displayType?: string
}

function DatasetManage() {
  const { id } = useParams()
  const navigate = useNavigate()
  const [dataset, setDataset] = useState<Dataset | null>(null)
  const [loading, setLoading] = useState(true)
  const [showAddTable, setShowAddTable] = useState(false)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [fileUrl, setFileUrl] = useState('')
  const [importMode, setImportMode] = useState<'file' | 'url'>('file')
  const [tableName, setTableName] = useState('')
  const [displayName, setDisplayName] = useState('')
  const [skipRows, setSkipRows] = useState('0')
  const [delimiter, setDelimiter] = useState('\t')
  const [primaryKey, setPrimaryKey] = useState('')
  const [uploading, setUploading] = useState(false)
  const [selectedTable, setSelectedTable] = useState<string | null>(null)
  const [tableData, setTableData] = useState<any[]>([])
  const [loadingData, setLoadingData] = useState(false)
  const [showColumnEditor, setShowColumnEditor] = useState(false)
  const [editingTableId, setEditingTableId] = useState<string | null>(null)
  const [columns, setColumns] = useState<ColumnMetadata[]>([])
  const [loadingColumns, setLoadingColumns] = useState(false)
  const [previewData, setPreviewData] = useState<any>(null)
  const [loadingPreview, setLoadingPreview] = useState(false)
  const [selectedPrimaryKey, setSelectedPrimaryKey] = useState('')
  const [confirmedRelationships, setConfirmedRelationships] = useState<any[]>([])
  const [showKeyEditor, setShowKeyEditor] = useState(false)
  const [keyEditorTableId, setKeyEditorTableId] = useState<string | null>(null)
  const [keyEditorColumns, setKeyEditorColumns] = useState<ColumnMetadata[]>([])
  const [keyEditorLoading, setKeyEditorLoading] = useState(false)
  const [primaryKeySelection, setPrimaryKeySelection] = useState('')
  const [initialPrimaryKeySelection, setInitialPrimaryKeySelection] = useState('')
  const [tableRelationships, setTableRelationships] = useState<Relationship[]>([])
  const [relationshipForm, setRelationshipForm] = useState({
    foreignKey: '',
    referencedTableId: '',
    referencedColumn: ''
  })
  const [showTemporalConfig, setShowTemporalConfig] = useState(false)
  const [temporalConfigTableId, setTemporalConfigTableId] = useState<string | null>(null)
  const [temporalColumns, setTemporalColumns] = useState<ColumnMetadata[]>([])
  const [referencedColumnsCache, setReferencedColumnsCache] = useState<Record<string, ColumnMetadata[]>>({})
  const [referencedColumnsLoading, setReferencedColumnsLoading] = useState(false)
  const [relationshipSaving, setRelationshipSaving] = useState(false)
  const [primaryKeySaving, setPrimaryKeySaving] = useState(false)

  useEffect(() => {
    fetchDataset()
  }, [id])

  const normalizeDataset = (raw: any): Dataset => {
    const rawTables = raw.tables || []
    const tableIdToDisplayName: Record<string, string> = {}
    const tableNameToDisplayName: Record<string, string> = {}

    rawTables.forEach((table: any) => {
      const displayName = table.displayName || table.tableDisplayName || table.name || table.table_name || table.id || table.table_id
      if (table.id) tableIdToDisplayName[table.id] = displayName
      if (table.table_id) tableIdToDisplayName[table.table_id] = displayName
      if (table.name) tableNameToDisplayName[table.name] = displayName
      if (table.table_name) tableNameToDisplayName[table.table_name] = displayName
    })

    return {
      ...raw,
      tables: rawTables.map((table: any) => ({
        ...table,
        relationships: (table.relationships || []).map((rel: any) => {
          const referencedTableKey = rel.referencedTable ?? rel.referenced_table
          const referencedDisplayName =
            rel.referencedTableDisplayName ??
            rel.referenced_table_display_name ??
            tableIdToDisplayName[referencedTableKey] ??
            tableNameToDisplayName[referencedTableKey] ??
            referencedTableKey

          return {
            foreignKey: rel.foreignKey ?? rel.foreign_key,
            referencedTable: referencedTableKey,
            referencedColumn: rel.referencedColumn ?? rel.referenced_column,
            type: rel.type ?? rel.relationship_type,
            referencedTableDisplayName: referencedDisplayName
          }
        })
      }))
    }
  }

  const fetchDataset = async (withLoading: boolean = true) => {
    try {
      if (withLoading) setLoading(true)
      const response = await api.get(`/datasets/${id}`)
      const loaded = normalizeDataset(response.data.dataset)
      setDataset(loaded)
      return loaded
    } catch (error) {
      console.error('Failed to load dataset:', error)
      return null
    } finally {
      if (withLoading) setLoading(false)
    }
  }

  const resolveTableDisplayName = (tableIdOrName: string) => {
    if (!dataset) return tableIdOrName
    const match = dataset.tables.find(
      (t) => t.id === tableIdOrName || t.name === tableIdOrName
    )
    return match?.displayName || match?.name || tableIdOrName
  }

  const hydrateRelationships = (relationships: Relationship[] = []) =>
    relationships.map((rel) => ({
      ...rel,
      referencedTableDisplayName: rel.referencedTableDisplayName || resolveTableDisplayName(rel.referencedTable)
    }))

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const file = e.target.files[0]
      setSelectedFile(file)
      if (!tableName) {
        const name = file.name.replace(/\.[^/.]+$/, '').replace(/[^a-z0-9_]/gi, '_').toLowerCase()
        setTableName(name)
        setDisplayName(file.name.replace(/\.[^/.]+$/, ''))
      }
      // Auto-trigger preview
      setTimeout(() => loadPreview(file, null), 100)
    }
  }

  const handleUrlChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const url = e.target.value
    setFileUrl(url)
    if (!tableName && url) {
      // Extract filename from URL
      const urlPath = url.split('?')[0]
      const filename = urlPath.substring(urlPath.lastIndexOf('/') + 1)
      const name = filename.replace(/\.[^/.]+$/, '').replace(/[^a-z0-9_]/gi, '_').toLowerCase()
      setTableName(name)
      setDisplayName(filename.replace(/\.[^/.]+$/, ''))
    }
  }

  const loadPreview = async (file: File | null, url: string | null) => {
    const formData = new FormData()

    if (file) {
      formData.append('file', file)
    } else if (url) {
      formData.append('fileUrl', url)
    } else {
      return
    }

    formData.append('skipRows', skipRows)
    formData.append('delimiter', delimiter)

    try {
      setLoadingPreview(true)
      const response = await api.post(`/datasets/${id}/tables/preview`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      })
      setPreviewData(response.data.preview)
      setConfirmedRelationships(response.data.preview.detectedRelationships || [])

      // Auto-detect skipRows if not manually set (still at default 0)
      if (skipRows === '0' && response.data.preview.detectedSkipRows !== undefined) {
        setSkipRows(String(response.data.preview.detectedSkipRows))
      }
    } catch (error: any) {
      console.error('Preview failed:', error)
      setPreviewData(null)
    } finally {
      setLoadingPreview(false)
    }
  }

  // Auto-reload preview when skipRows or delimiter changes
  useEffect(() => {
    if (selectedFile || fileUrl) {
      const timer = setTimeout(() => {
        loadPreview(selectedFile, fileUrl)
      }, 500) // Debounce
      return () => clearTimeout(timer)
    }
  }, [skipRows, delimiter])

  const handleAddTable = async (e: React.FormEvent) => {
    e.preventDefault()
    if (importMode === 'file' && !selectedFile) return
    if (importMode === 'url' && !fileUrl) return
    if (!tableName) return

    const formData = new FormData()

    if (importMode === 'file' && selectedFile) {
      formData.append('file', selectedFile)
    } else if (importMode === 'url') {
      formData.append('fileUrl', fileUrl)
    }

    formData.append('tableName', tableName)
    formData.append('displayName', displayName || tableName)
    formData.append('skipRows', skipRows)
    formData.append('delimiter', delimiter)

    // Use selected primary key from preview or manual input
    const finalPrimaryKey = selectedPrimaryKey || primaryKey
    if (finalPrimaryKey) formData.append('primaryKey', finalPrimaryKey)

    // Add confirmed relationships
    if (confirmedRelationships.length > 0) {
      const relationships = confirmedRelationships.map(rel => ({
        foreign_key: rel.foreignKey,
        referenced_table: rel.referencedTable,
        referenced_column: rel.referencedColumn
      }))
      formData.append('relationships', JSON.stringify(relationships))
    }

    try {
      setUploading(true)
      await api.post(`/datasets/${id}/tables`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 600000 // 10 minute timeout for large files
      })
      setShowAddTable(false)
      setSelectedFile(null)
      setFileUrl('')
      setTableName('')
      setDisplayName('')
      setSkipRows('0')
      setPrimaryKey('')
      await fetchDataset()
    } catch (error: any) {
      console.error('Add table failed:', error)
      alert('Add table failed: ' + (error.response?.data?.message || error.message))
    } finally {
      setUploading(false)
    }
  }

  const loadTableData = async (tableId: string) => {
    try {
      setLoadingData(true)
      setSelectedTable(tableId)
      const response = await api.get(`/datasets/${id}/tables/${tableId}/data?limit=100`)
      setTableData(response.data.data)
    } catch (error) {
      console.error('Failed to load table data:', error)
    } finally {
      setLoadingData(false)
    }
  }

  const handleDeleteTable = async (tableId: string) => {
    if (!confirm('Are you sure you want to delete this table?')) return

    try {
      await api.delete(`/datasets/${id}/tables/${tableId}`)
      await fetchDataset()
      if (selectedTable === tableId) {
        setSelectedTable(null)
        setTableData([])
      }
    } catch (error) {
      console.error('Delete table failed:', error)
      alert('Failed to delete table')
    }
  }

  const loadColumns = async (tableId: string) => {
    try {
      setLoadingColumns(true)
      const cols = await fetchTableColumns(tableId)
      setColumns(cols)
      setEditingTableId(tableId)
      setShowColumnEditor(true)
    } catch (error) {
      console.error('Failed to load columns:', error)
      alert('Failed to load columns')
    } finally {
      setLoadingColumns(false)
    }
  }

  const loadTemporalConfig = async (tableId: string) => {
    try {
      setLoadingColumns(true)
      const cols = await fetchTableColumns(tableId)
      setTemporalColumns(cols)
      setTemporalConfigTableId(tableId)
      setShowTemporalConfig(true)
    } catch (error) {
      console.error('Failed to load temporal config:', error)
      alert('Failed to load temporal configuration')
    } finally {
      setLoadingColumns(false)
    }
  }

  const fetchTableColumns = async (tableId: string) => {
    const response = await api.get(`/datasets/${id}/tables/${tableId}/columns`)
    return response.data.columns as ColumnMetadata[]
  }

  const openKeyEditor = async (table: Table) => {
    setKeyEditorTableId(table.id)
    const initialPk = table.primaryKey || ''
    setPrimaryKeySelection(initialPk)
    setInitialPrimaryKeySelection(initialPk)
    setRelationshipForm({ foreignKey: '', referencedTableId: '', referencedColumn: '' })
    setReferencedColumnsCache({})
    setTableRelationships(hydrateRelationships(table.relationships || []))
    setShowKeyEditor(true)
    setKeyEditorLoading(true)
    try {
      const cols = await fetchTableColumns(table.id)
      setKeyEditorColumns(cols)
    } catch (error) {
      console.error('Failed to load columns for key editor:', error)
      alert('Failed to load columns for key editor')
      setShowKeyEditor(false)
    } finally {
      setKeyEditorLoading(false)
    }
  }

  const closeKeyEditor = () => {
    setShowKeyEditor(false)
    setKeyEditorTableId(null)
    setKeyEditorColumns([])
    setInitialPrimaryKeySelection('')
    setTableRelationships([])
    setRelationshipForm({ foreignKey: '', referencedTableId: '', referencedColumn: '' })
    setReferencedColumnsCache({})
  }

  const ensureReferencedColumns = async (tableId: string) => {
    if (referencedColumnsCache[tableId]) {
      return referencedColumnsCache[tableId]
    }
    setReferencedColumnsLoading(true)
    try {
      const cols = await fetchTableColumns(tableId)
      setReferencedColumnsCache(prev => ({ ...prev, [tableId]: cols }))
      return cols
    } catch (error) {
      console.error('Failed to load referenced table columns:', error)
      alert('Failed to load referenced table columns')
      return []
    } finally {
      setReferencedColumnsLoading(false)
    }
  }

  const handleSavePrimaryKey = async () => {
    if (!keyEditorTableId) return
    if (primaryKeySelection === initialPrimaryKeySelection) return
    try {
      setPrimaryKeySaving(true)
      await api.patch(`/datasets/${id}/tables/${keyEditorTableId}/primary-key`, {
        primaryKey: primaryKeySelection || null
      })
      const updated = await fetchDataset(false)
      if (updated) {
        const table = updated.tables.find(t => t.id === keyEditorTableId)
        if (table) {
          setTableRelationships(hydrateRelationships(table.relationships || []))
          const newPrimaryKey = table.primaryKey || ''
          setPrimaryKeySelection(newPrimaryKey)
          setInitialPrimaryKeySelection(newPrimaryKey)
        }
      }
    } catch (error) {
      console.error('Failed to save primary key:', error)
      alert('Failed to save primary key')
    } finally {
      setPrimaryKeySaving(false)
    }
  }

  const handleAddRelationship = async () => {
    if (!keyEditorTableId) return
    const { foreignKey, referencedTableId, referencedColumn } = relationshipForm
    if (!foreignKey || !referencedTableId || !referencedColumn) {
      alert('Please select a column, referenced table, and referenced column')
      return
    }
    try {
      setRelationshipSaving(true)
      const referencedTableName =
        dataset?.tables.find((t) => t.id === referencedTableId)?.name || referencedTableId
      await api.post(`/datasets/${id}/tables/${keyEditorTableId}/relationships`, {
        foreignKey,
        referencedTableId,
        referencedTable: referencedTableName,
        referencedColumn
      })
      const updated = await fetchDataset(false)
      if (updated) {
        const table = updated.tables.find(t => t.id === keyEditorTableId)
        if (table) {
          setTableRelationships(hydrateRelationships(table.relationships || []))
        }
      }
      setRelationshipForm({ foreignKey: '', referencedTableId: '', referencedColumn: '' })
    } catch (error) {
      console.error('Failed to add relationship:', error)
      alert('Failed to add relationship')
    } finally {
      setRelationshipSaving(false)
    }
  }

  const handleDeleteRelationship = async (rel: Relationship) => {
    if (!keyEditorTableId) return
    try {
      setRelationshipSaving(true)
      await api.delete(`/datasets/${id}/tables/${keyEditorTableId}/relationships`, {
        params: {
          foreignKey: rel.foreignKey,
          referencedTable: rel.referencedTable,
          referencedColumn: rel.referencedColumn
        }
      })
      const updated = await fetchDataset(false)
      if (updated) {
        const table = updated.tables.find(t => t.id === keyEditorTableId)
        if (table) {
          setTableRelationships(hydrateRelationships(table.relationships || []))
        }
      }
    } catch (error) {
      console.error('Failed to delete relationship:', error)
      alert('Failed to delete relationship')
    } finally {
      setRelationshipSaving(false)
    }
  }

  const updateColumnMetadata = async (columnName: string, updates: ColumnMetadataUpdate) => {
    if (!editingTableId) return

    try {
      // Optimistically update local state
      setColumns(prevColumns =>
        prevColumns.map(col =>
          col.column_name === columnName
            ? {
                ...col,
                display_name: updates.displayName !== undefined ? updates.displayName : col.display_name,
                description: updates.description !== undefined ? updates.description : col.description,
                is_hidden: updates.isHidden !== undefined ? updates.isHidden : col.is_hidden,
                display_type: updates.displayType !== undefined ? updates.displayType : col.display_type
              }
            : col
        )
      )

      // Save to server
      await api.patch(`/datasets/${id}/tables/${editingTableId}/columns/${columnName}`, updates)
    } catch (error) {
      console.error('Failed to update column:', error)
      alert('Failed to update column metadata')
      // Reload columns on error to revert optimistic update
      await loadColumns(editingTableId)
    }
  }

  const primaryKeySaveDisabled = primaryKeySaving || primaryKeySelection === initialPrimaryKeySelection
  const relationshipActionDisabled =
    relationshipSaving ||
    keyEditorLoading ||
    !relationshipForm.foreignKey ||
    !relationshipForm.referencedTableId ||
    !relationshipForm.referencedColumn

  if (loading) return <p>Loading dataset...</p>

  if (!dataset) return <p>Dataset not found</p>

  return (
    <div>
      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
        <button
          onClick={() => navigate(-1)}
          style={{
            padding: '0.5rem 1rem',
            background: '#666',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer'
          }}
        >
          ← Back
        </button>
        <button
          onClick={() => navigate(`/datasets/${id}`)}
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
          onClick={async () => {
            if (confirm('Are you sure you want to delete this dataset and all its tables?')) {
              try {
                await api.delete(`/datasets/${id}`)
                navigate('/datasets')
              } catch (error) {
                console.error('Delete failed:', error)
                alert('Failed to delete dataset')
              }
            }
          }}
          style={{
            padding: '0.5rem 1rem',
            background: '#f44336',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            marginLeft: 'auto'
          }}
        >
          Delete Dataset
        </button>
      </div>

      <div style={{ marginBottom: '2rem', background: 'white', padding: '1.5rem', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
        <h2 style={{ marginTop: 0 }}>{dataset.name}</h2>
        {dataset.description && (
          <SafeHtml
            html={dataset.description}
            style={{ color: '#666', display: 'block', margin: '0.5rem 0 0 0' }}
          />
        )}

        {dataset.tags && dataset.tags.length > 0 && (
          <div style={{ marginTop: '1rem', display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            <strong style={{ marginRight: '0.5rem' }}>Tags:</strong>
            {dataset.tags.map((tag, i) => (
              <span
                key={i}
                style={{
                  padding: '0.25rem 0.5rem',
                  background: '#e3f2fd',
                  color: '#1976d2',
                  borderRadius: '4px',
                  fontSize: '0.875rem',
                  fontWeight: 500
                }}
              >
                {tag}
              </span>
            ))}
          </div>
        )}

        {dataset.source && (
          <div style={{ marginTop: '0.75rem', fontSize: '0.875rem', color: '#666' }}>
            <strong>Source:</strong> {dataset.source}
          </div>
        )}

        {dataset.citation && (
          <div style={{ marginTop: '0.5rem', fontSize: '0.875rem', color: '#666' }}>
            <strong>Citation:</strong> {dataset.citation}
          </div>
        )}

        {dataset.references && dataset.references.length > 0 && (
          <div style={{ marginTop: '0.75rem', fontSize: '0.875rem', color: '#666' }}>
            <strong>References:</strong>
            <ul style={{ margin: '0.5rem 0 0 1.5rem', padding: 0 }}>
              {dataset.references.map((ref, i) => (
                <li key={i} style={{ marginBottom: '0.25rem' }}>
                  {ref.startsWith('pmid:') ? (
                    <a href={`https://pubmed.ncbi.nlm.nih.gov/${ref.substring(5)}/`} target="_blank" rel="noopener noreferrer" style={{ color: '#1976d2' }}>
                      {ref}
                    </a>
                  ) : ref.startsWith('doi:') ? (
                    <a href={`https://doi.org/${ref.substring(4)}`} target="_blank" rel="noopener noreferrer" style={{ color: '#1976d2' }}>
                      {ref}
                    </a>
                  ) : (
                    ref
                  )}
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>

      <div style={{ marginBottom: '2rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
          <h3>Tables ({dataset.tables.length})</h3>
          <button
            onClick={() => setShowAddTable(!showAddTable)}
            style={{
              padding: '0.5rem 1rem',
              background: '#4CAF50',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            {showAddTable ? 'Cancel' : '+ Add Table'}
          </button>
        </div>

        {showAddTable && (
          <div style={{
            background: 'white',
            padding: '1.5rem',
            borderRadius: '8px',
            marginBottom: '1rem',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
          }}>
            <h4 style={{ marginTop: 0 }}>Add Table to Dataset</h4>
            <form onSubmit={handleAddTable}>
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
                  <input type="file" accept=".csv,.txt,.tsv" onChange={handleFileSelect} required />
                </div>
              ) : (
                <div style={{ marginBottom: '1rem' }}>
                  <label style={{ display: 'block', marginBottom: '0.5rem' }}>File URL</label>
                  <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <input
                      type="url"
                      value={fileUrl}
                      onChange={handleUrlChange}
                      onBlur={() => fileUrl && loadPreview(null, fileUrl)}
                      placeholder=""
                      style={{ flex: 1, padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
                      required
                    />
                    <button
                      type="button"
                      onClick={() => fileUrl && loadPreview(null, fileUrl)}
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
                    Provide a direct URL to a CSV, TSV, or TXT file and click Load
                  </small>
                </div>
              )}

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
                <div>
                  <label style={{ display: 'block', marginBottom: '0.5rem' }}>Table Name (identifier) *</label>
                  <input
                    type="text"
                    value={tableName}
                    onChange={(e) => setTableName(e.target.value)}
                    required
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
                    onChange={(e) => setDelimiter(e.target.value)}
                    style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
                  >
                    <option value="\t">Tab</option>
                    <option value=",">Comma</option>
                    <option value=";">Semicolon</option>
                    <option value="|">Pipe</option>
                  </select>
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

              {/* Preview Section */}
              {loadingPreview && (
                <div style={{ padding: '1rem', background: '#f5f5f5', borderRadius: '4px', marginBottom: '1rem', textAlign: 'center' }}>
                  Loading preview...
                </div>
              )}

              {previewData && (
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
                      {previewData.columns.map((col: any) => (
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
                          {previewData.detectedRelationships.map((rel: any, idx: number) => (
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
                      !previewData.detectedRelationships?.some((dr: any) =>
                        dr.foreignKey === r.foreignKey && dr.referencedTable === r.referencedTable
                      )
                    ).length > 0 && (
                      <div style={{ marginBottom: '0.5rem' }}>
                        <div style={{ fontSize: '0.875rem', color: '#666', marginBottom: '0.5rem' }}>
                          Manually added:
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                          {confirmedRelationships.filter(r =>
                            !previewData.detectedRelationships?.some((dr: any) =>
                              dr.foreignKey === r.foreignKey && dr.referencedTable === r.referencedTable
                            )
                          ).map((rel: any, idx: number) => (
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
                                id="manual-fk-column"
                                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd', fontSize: '0.875rem' }}
                              >
                                <option value="">Select...</option>
                                {previewData.columns.map((col: any) => (
                                  <option key={col.name} value={col.name}>{col.name}</option>
                                ))}
                              </select>
                            </div>
                            <div>
                              <label style={{ display: 'block', fontSize: '0.75rem', marginBottom: '0.25rem' }}>
                                References Table
                              </label>
                              <select
                                id="manual-fk-table"
                                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd', fontSize: '0.875rem' }}
                                onChange={(e) => {
                                  const table = dataset.tables.find(t => t.id === e.target.value)
                                  const colSelect = document.getElementById('manual-fk-ref-column') as HTMLSelectElement
                                  if (colSelect && table) {
                                    colSelect.innerHTML = '<option value="">Select...</option>' +
                                      table.columns.map(c => `<option value="${c.name}">${c.name}</option>`).join('')
                                  }
                                }}
                              >
                                <option value="">Select...</option>
                                {dataset.tables.map((table: any) => (
                                  <option key={table.id} value={table.id}>{table.displayName}</option>
                                ))}
                              </select>
                            </div>
                            <div>
                              <label style={{ display: 'block', fontSize: '0.75rem', marginBottom: '0.25rem' }}>
                                References Column
                              </label>
                              <select
                                id="manual-fk-ref-column"
                                style={{ width: '100%', padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd', fontSize: '0.875rem' }}
                              >
                                <option value="">Select...</option>
                              </select>
                            </div>
                            <button
                              type="button"
                              onClick={() => {
                                const colSelect = document.getElementById('manual-fk-column') as HTMLSelectElement
                                const tableSelect = document.getElementById('manual-fk-table') as HTMLSelectElement
                                const refColSelect = document.getElementById('manual-fk-ref-column') as HTMLSelectElement

                                const foreignKey = colSelect?.value
                                const tableId = tableSelect?.value
                                const referencedColumn = refColSelect?.value

                                if (foreignKey && tableId && referencedColumn) {
                                  const table = dataset.tables.find(t => t.id === tableId)
                                  if (table) {
                                    const newRel = {
                                      foreignKey,
                                      referencedTable: table.name,
                                      referencedTableId: table.id,
                                      referencedColumn,
                                      matchPercentage: 100,
                                      sampleMatches: []
                                    }
                                    setConfirmedRelationships([...confirmedRelationships, newRel])
                                    colSelect.value = ''
                                    tableSelect.value = ''
                                    refColSelect.value = ''
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

                  {/* Sample Data */}
                  <details open>
                    <summary style={{ cursor: 'pointer', fontWeight: 'bold', marginBottom: '0.5rem' }}>
                      Sample Data (first 10 rows)
                    </summary>
                    <div style={{ overflowX: 'auto', maxHeight: '300px', overflowY: 'auto', background: 'white', borderRadius: '4px' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem' }}>
                        <thead style={{ position: 'sticky', top: 0, background: '#f5f5f5' }}>
                          <tr>
                            {previewData.columns.map((col: any) => (
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
                          {previewData.sampleRows.map((row: any[], rowIdx: number) => (
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
              )}

              <button
                type="submit"
                disabled={uploading || !previewData || (importMode === 'file' && !selectedFile) || (importMode === 'url' && !fileUrl)}
                style={{
                  padding: '0.75rem 1.5rem',
                  background: uploading ? '#ccc' : '#4CAF50',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: uploading ? 'not-allowed' : 'pointer'
                }}
              >
                {uploading ? 'Adding...' : 'Add Table'}
              </button>
            </form>
          </div>
        )}

        <div style={{ display: 'grid', gap: '1rem' }}>
          {dataset.tables.map((table) => (
            <div
              key={table.id}
              style={{
                background: 'white',
                padding: '1.5rem',
                borderRadius: '8px',
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                border: selectedTable === table.id ? '2px solid #2196F3' : '2px solid transparent'
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', marginBottom: '1rem' }}>
                <div>
                  <h4 style={{ margin: '0 0 0.5rem 0' }}>{table.displayName}</h4>
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
                          {rel.foreignKey} → {(rel.referencedTableDisplayName || resolveTableDisplayName(rel.referencedTable))}.{rel.referencedColumn} ({rel.type || 'many-to-one'})
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  <button
                    onClick={() => loadTableData(table.id)}
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
                    onClick={() => openKeyEditor(table)}
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
                    onClick={() => loadColumns(table.id)}
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
                    onClick={() => loadTemporalConfig(table.id)}
                    style={{
                      padding: '0.5rem 1rem',
                      background: '#009688',
                      color: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      cursor: 'pointer'
                    }}
                  >
                    Temporal Config
                  </button>
                  <button
                    onClick={() => handleDeleteTable(table.id)}
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
          ))}
        </div>
      </div>

      {selectedTable && (
        <div style={{ marginTop: '2rem' }}>
          <h3>Table Data Preview</h3>
          {loadingData ? (
            <p>Loading data...</p>
          ) : tableData.length > 0 ? (
            <div style={{ overflowX: 'auto', background: 'white', padding: '1rem', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem' }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid #ddd' }}>
                    {Object.keys(tableData[0]).map((key) => (
                      <th key={key} style={{ padding: '0.5rem', textAlign: 'left', background: '#f5f5f5' }}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {tableData.slice(0, 50).map((row, idx) => (
                    <tr key={idx} style={{ borderBottom: '1px solid #eee' }}>
                      {Object.values(row).map((val: any, i) => (
                        <td key={i} style={{ padding: '0.5rem' }}>{val?.toString() || '-'}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              {tableData.length > 50 && (
                <p style={{ marginTop: '1rem', color: '#666', fontSize: '0.875rem' }}>
                  Showing first 50 of {tableData.length} rows
                </p>
              )}
            </div>
          ) : (
            <p>No data available</p>
          )}
        </div>
      )}

      {/* Column Editor Modal */}
      {showColumnEditor && (
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
                onClick={() => setShowColumnEditor(false)}
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
                  <div key={col.column_name} style={{
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
                              updateColumnMetadata(col.column_name, { displayName: e.target.value })
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
                              updateColumnMetadata(col.column_name, { description: e.target.value })
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
                            updateColumnMetadata(col.column_name, { displayType: e.target.value })
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
                              updateColumnMetadata(col.column_name, { isHidden: e.target.checked })
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
      )}

      {/* Key & Relationships Editor Modal */}
      {showKeyEditor && (
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
                onClick={closeKeyEditor}
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
                      onClick={handleSavePrimaryKey}
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
                            <strong>{rel.foreignKey}</strong> → {(rel.referencedTableDisplayName || resolveTableDisplayName(rel.referencedTable))}.{rel.referencedColumn}
                          </div>
                          <button
                            type="button"
                            onClick={() => handleDeleteRelationship(rel)}
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
                              await ensureReferencedColumns(value)
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
                          onClick={handleAddRelationship}
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
      )}

      {/* Temporal Configuration Panel */}
      {showTemporalConfig && temporalConfigTableId && (
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
          onClick={() => setShowTemporalConfig(false)}
        >
          <div
            style={{
              background: 'white',
              borderRadius: '12px',
              maxWidth: '1200px',
              width: '90%',
              maxHeight: '90vh',
              overflow: 'auto',
              boxShadow: '0 4px 20px rgba(0,0,0,0.3)'
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ padding: '20px', borderBottom: '1px solid #e0e0e0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h2 style={{ margin: 0 }}>Temporal Configuration</h2>
              <button
                onClick={() => setShowTemporalConfig(false)}
                style={{
                  background: 'none',
                  border: 'none',
                  fontSize: '24px',
                  cursor: 'pointer',
                  color: '#666',
                  padding: '0',
                  width: '32px',
                  height: '32px'
                }}
              >
                ×
              </button>
            </div>
            <TemporalConfigPanel
              datasetId={id!}
              tableId={temporalConfigTableId}
              columns={temporalColumns}
              onUpdate={() => loadTemporalConfig(temporalConfigTableId)}
            />
          </div>
        </div>
      )}
    </div>
  )
}

export default DatasetManage

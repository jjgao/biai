import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import SafeHtml from '../components/SafeHtml'
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
  display_name: string
  description: string
  is_hidden: boolean
  display_type: string
  suggested_chart: string
}

interface ColumnMetadataUpdate {
  displayName?: string
  description?: string
  isHidden?: boolean
  displayType?: string
}

interface SheetInfo {
  name: string
  rowCount: number
  preview?: any[][]
  columns?: string[]
}

interface SpreadsheetPreview {
  filename: string
  sheets: SheetInfo[]
}

interface SheetImportConfig {
  sheetName: string
  tableName: string
  displayName: string
  selected: boolean
  skipRows: number
  primaryKey: string
  relationships: Relationship[]
  importMode: 'append' | 'replace' | 'upsert'
  targetTableId: string
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
  const [referencedColumnsCache, setReferencedColumnsCache] = useState<Record<string, ColumnMetadata[]>>({})
  const [referencedColumnsLoading, setReferencedColumnsLoading] = useState(false)
  const [relationshipSaving, setRelationshipSaving] = useState(false)
  const [primaryKeySaving, setPrimaryKeySaving] = useState(false)
  const [selectedListColumns, setSelectedListColumns] = useState<Map<string, 'python' | 'json'>>(new Map())
  const [wasDelimiterDetected, setWasDelimiterDetected] = useState(false)
  const [detectedDelimiterName, setDetectedDelimiterName] = useState<string>('')
  
  // Import configuration
  const [importTarget, setImportTarget] = useState<'new' | 'existing'>('new')
  const [targetTableId, setTargetTableId] = useState('')
  const [importModeType, setImportModeType] = useState<'append' | 'replace' | 'upsert'>('append')

  // Spreadsheet specific state
  const [isSpreadsheet, setIsSpreadsheet] = useState(false)
  const [spreadsheetPreview, setSpreadsheetPreview] = useState<SpreadsheetPreview | null>(null)
  const [sheetConfigs, setSheetConfigs] = useState<SheetImportConfig[]>([])

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
      // Reset auto-detect indicator when new file is selected
      setWasDelimiterDetected(false)
      setDetectedDelimiterName('')
      
      const isSheet = file.name.match(/\.(xlsx|xls|ods)$/i)
      setIsSpreadsheet(!!isSheet)
      
      if (!tableName) {
        const name = file.name.replace(/\.[^/.]+$/, '').replace(/[^a-z0-9_]/gi, '_').toLowerCase()
        setTableName(name)
        setDisplayName(file.name.replace(/\.[^/.]+$/, ''))
      }
      
      // Auto-trigger preview
      if (isSheet) {
        setTimeout(() => loadSpreadsheetPreview(file, null), 100)
      } else {
        setTimeout(() => loadPreview(file, null), 100)
      }
    }
  }

  const handleUrlChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const url = e.target.value
    setFileUrl(url)
    
    const isSheet = url.split('?')[0].match(/\.(xlsx|xls|ods)$/i)
    setIsSpreadsheet(!!isSheet)
    
    if (!tableName && url) {
      // Extract filename from URL
      const urlPath = url.split('?')[0]
      const filename = urlPath.substring(urlPath.lastIndexOf('/') + 1)
      const name = filename.replace(/\.[^/.]+$/, '').replace(/[^a-z0-9_]/gi, '_').toLowerCase()
      setTableName(name)
      setDisplayName(filename.replace(/\.[^/.]+$/, ''))
    }
  }

  const loadSpreadsheetPreview = async (file: File | null, url: string | null) => {
    const formData = new FormData()

    if (file) {
      formData.append('file', file)
    } else if (url) {
      formData.append('fileUrl', url)
    } else {
      return
    }

    try {
      setLoadingPreview(true)
      const response = await api.post(`/datasets/${id}/spreadsheets/preview`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      })
      setSpreadsheetPreview(response.data.preview)
      setPreviewData(null)
      setSheetConfigs(response.data.preview.sheets.map((sheet: any) => ({
        sheetName: sheet.name,
        tableName: sheet.name.replace(/[^a-z0-9_]/gi, '_').toLowerCase(),
        displayName: sheet.name,
        selected: sheet.rowCount > 1, // Select by default if it has data
        skipRows: 0,
        primaryKey: sheet.detectedPrimaryKey || '',
        relationships: (sheet.detectedRelationships || []).map((rel: any) => ({
          foreignKey: rel.foreignKey,
          referencedTable: rel.referencedTableId,
          referencedColumn: rel.referencedColumn,
          type: 'many-to-one',
          referencedTableDisplayName: rel.referencedTable
        })),
        importMode: 'append',
        targetTableId: ''
      })))
    } catch (error: any) {
      console.error('Spreadsheet preview failed:', error)
      setSpreadsheetPreview(null)
    } finally {
      setLoadingPreview(false)
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
      setSpreadsheetPreview(null)
      setConfirmedRelationships(response.data.preview.detectedRelationships || [])

      // Auto-select high-confidence list columns
      const listSuggestions = response.data.preview.listSuggestions || []
      const autoSelectedLists = new Map<string, 'python' | 'json'>()
      listSuggestions.forEach((suggestion: any) => {
        if (suggestion.confidence === 'high') {
          autoSelectedLists.set(suggestion.columnName, suggestion.listSyntax)
        }
      })
      setSelectedListColumns(autoSelectedLists)

      // Auto-detect delimiter if detected
      if (response.data.preview.detectedDelimiter !== undefined) {
        const detected = response.data.preview.detectedDelimiter
        setDelimiter(detected)
        setWasDelimiterDetected(true)

        // Set human-readable name
        const delimiterNames: Record<string, string> = {
          ',': 'Comma',
          '\t': 'Tab',
          ';': 'Semicolon',
          '|': 'Pipe'
        }
        setDetectedDelimiterName(delimiterNames[detected] || detected)
      }

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

  const handleSpreadsheetImport = async () => {
    if (importMode === 'file' && !selectedFile) return
    if (importMode === 'url' && !fileUrl) return
    
    const selectedSheets = sheetConfigs.filter(s => s.selected).map(s => ({
      sheetName: s.sheetName,
      tableName: s.tableName,
      displayName: s.displayName,
      skipRows: s.skipRows,
      primaryKey: s.primaryKey,
      relationships: s.relationships,
      importMode: s.importMode,
      targetTableId: s.targetTableId
    }))

    if (selectedSheets.length === 0) {
      alert('Please select at least one sheet to import')
      return
    }

    const formData = new FormData()

    if (importMode === 'file' && selectedFile) {
      formData.append('file', selectedFile)
    } else if (importMode === 'url') {
      formData.append('fileUrl', fileUrl)
    }

    formData.append('sheetsConfig', JSON.stringify(selectedSheets))

    try {
      setUploading(true)
      await api.post(`/datasets/${id}/spreadsheets/import`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 600000
      })
      setShowAddTable(false)
      setSelectedFile(null)
      setFileUrl('')
      setIsSpreadsheet(false)
      setSpreadsheetPreview(null)
      await fetchDataset()
    } catch (error: any) {
      console.error('Spreadsheet import failed:', error)
      alert('Spreadsheet import failed: ' + (error.response?.data?.message || error.message))
    } finally {
      setUploading(false)
    }
  }

  const removeSheetRelationship = (sheetIdx: number, relIdx: number) => {
    const newConfigs = [...sheetConfigs]
    newConfigs[sheetIdx].relationships = newConfigs[sheetIdx].relationships.filter((_, i) => i !== relIdx)
    setSheetConfigs(newConfigs)
  }

  const addSheetRelationship = (sheetIdx: number, rel: Relationship) => {
    const newConfigs = [...sheetConfigs]
    newConfigs[sheetIdx].relationships.push(rel)
    setSheetConfigs(newConfigs)
  }

  const getPotentialTargets = (currentSheetIdx: number) => {
    const targets = []
    // Existing tables
    if (dataset) {
      targets.push(...dataset.tables.map(t => ({ 
        id: t.id, 
        name: t.name, 
        displayName: t.displayName, 
        columns: t.columns.map(c => c.name) 
      })))
    }
    // Other selected sheets
    if (spreadsheetPreview && sheetConfigs) {
      spreadsheetPreview.sheets.forEach((s, idx) => {
        if (idx !== currentSheetIdx && sheetConfigs[idx]?.selected) {
           targets.push({
             id: sheetConfigs[idx].tableName,
             name: sheetConfigs[idx].tableName,
             displayName: `[New] ${sheetConfigs[idx].displayName}`,
             columns: s.columns || []
           })
        }
      })
    }
    return targets
  }

  const handleAddTable = async (e: React.FormEvent) => {
    e.preventDefault()
    
    if (isSpreadsheet) {
      return handleSpreadsheetImport()
    }

    if (importMode === 'file' && !selectedFile) return
    if (importMode === 'url' && !fileUrl) return
    if (importTarget === 'new' && !tableName) return
    if (importTarget === 'existing' && !targetTableId) return

    const formData = new FormData()

    if (importMode === 'file' && selectedFile) {
      formData.append('file', selectedFile)
    } else if (importMode === 'url') {
      formData.append('fileUrl', fileUrl)
    }

    if (importTarget === 'existing') {
      formData.append('targetTableId', targetTableId)
      formData.append('importMode', importModeType)
      const targetTable = dataset?.tables.find(t => t.id === targetTableId)
      formData.append('tableName', targetTable?.name || 'existing_table')
    } else {
      formData.append('tableName', tableName)
      formData.append('displayName', displayName || tableName)
    }

    formData.append('skipRows', skipRows)
    formData('delimiter', delimiter)

    // Use selected primary key from preview or manual input
    const finalPrimaryKey = selectedPrimaryKey || primaryKey
    if (finalPrimaryKey) formData.append('primaryKey', finalPrimaryKey)

    // Add confirmed relationships
    if (confirmedRelationships.length > 0) {
      const relationships = confirmedRelationships.map(rel => ({
        foreignKey: rel.foreignKey,
        referenced_table: rel.referencedTable,
        referenced_column: rel.referenced_column
      }))
      formData.append('relationships', JSON.stringify(relationships))
    }

    // Add selected list columns
    if (selectedListColumns.size > 0) {
      const listColumnsObj = Object.fromEntries(selectedListColumns)
      formData.append('listColumns', JSON.stringify(listColumnsObj))
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
      setImportTarget('new')
      setTargetTableId('')
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
                  <input type="file" accept=".csv,.txt,.tsv,.xlsx,.xls,.ods" onChange={handleFileSelect} required />
                </div>
              ) : (
                <div style={{ marginBottom: '1rem' }}>
                  <label style={{ display: 'block', marginBottom: '0.5rem' }}>File URL</label>
                  <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <input
                      type="url"
                      value={fileUrl}
                      onChange={handleUrlChange}
                      onBlur={() => {
                        if (fileUrl) {
                          const isSheet = fileUrl.split('?')[0].match(/\.(xlsx|xls|ods)$/i)
                          if (isSheet) loadSpreadsheetPreview(null, fileUrl)
                          else loadPreview(null, fileUrl)
                        }
                      }}
                      placeholder=""
                      style={{ flex: 1, padding: '0.5rem', borderRadius: '4px', border: '1px solid #ddd' }}
                      required
                    />
                    <button
                      type="button"
                      onClick={() => {
                        if (fileUrl) {
                          const isSheet = fileUrl.split('?')[0].match(/\.(xlsx|xls|ods)$/i)
                          if (isSheet) loadSpreadsheetPreview(null, fileUrl)
                          else loadPreview(null, fileUrl)
                        }
                      }}
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

              {!isSpreadsheet && (
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
                          // If user manually changes delimiter, clear auto-detect indicator
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
              )}

              {/* Spreadsheet Section */}
              {isSpreadsheet && spreadsheetPreview && (
                <div style={{ marginBottom: '1rem', padding: '1.5rem', background: '#f9f9f9', borderRadius: '4px', border: '1px solid #ddd' }}>
                  <h4 style={{ marginTop: 0, marginBottom: '1rem' }}>Spreadsheet Sheets ({sheetConfigs.length})</h4>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                    {sheetConfigs.map((config, idx) => (
                      <div key={idx} style={{ padding: '1rem', background: 'white', border: '1px solid #eee', borderRadius: '4px' }}>
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
                                    newConfigs[idx].targetTableId = e.target.value
                                    if (e.target.value) {
                                      // If selecting existing table, default to append
                                      newConfigs[idx].importMode = 'append'
                                    } else {
                                      newConfigs[idx].importMode = 'append' // Reset
                                    }
                                    setSheetConfigs(newConfigs)
                                  }}
                                  style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd' }}
                                >
                                  <option value="">New Table</option>
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
                                  style={{ width: '100%', padding: '0.25rem 0.5rem', fontSize: '0.875rem', borderRadius: '4px', border: '1px solid #ddd' }}
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

                            <div style={{ marginBottom: '1rem' }}>
                              <label style={{ display: 'block', fontSize: '0.75rem', color: '#666', marginBottom: '0.25rem' }}>Relationships</label>
                              
                              {/* List existing */}
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

                              {/* Add new */}
                              <details>
                                <summary style={{ fontSize: '0.75rem', cursor: 'pointer', color: '#2196F3' }}>+ Add Relationship</summary>
                                <div style={{ marginTop: '0.5rem', padding: '0.5rem', background: '#f5f5f5', borderRadius: '4px', display: 'grid', gridTemplateColumns: '1fr 1fr 1fr auto', gap: '0.5rem', alignItems: 'end' }}>
                                  <div>
                                    <label style={{ display: 'block', fontSize: '0.7rem' }}>Col</label>
                                    <select id={`sheet-fk-col-${idx}`} style={{ width: '100%', fontSize: '0.75rem', padding: '0.25rem' }}>
                                      <option value="">Select...</option>
                                      {spreadsheetPreview.sheets[idx].columns?.map(c => <option key={c} value={c}>{c}</option>)}
                                    </select>
                                  </div>
                                  <div>
                                    <label style={{ display: 'block', fontSize: '0.7rem' }}>Ref Table</label>
                                    <select 
                                      id={`sheet-fk-table-${idx}`} 
                                      style={{ width: '100%', fontSize: '0.75rem', padding: '0.25rem' }}
                                      onChange={(e) => {
                                        const targets = getPotentialTargets(idx)
                                        const target = targets.find(t => t.id === e.target.value)
                                        const colSelect = document.getElementById(`sheet-fk-refcol-${idx}`) as HTMLSelectElement
                                        if (colSelect && target) {
                                          colSelect.innerHTML = '<option value="">Select...</option>' + 
                                            target.columns.map(c => `<option value="${c}">${c}</option>`).join('')
                                        }
                                      }}
                                    >
                                      <option value="">Select...</option>
                                      {getPotentialTargets(idx).map(t => (
                                        <option key={t.id} value={t.id}>{t.displayName}</option>
                                      ))}
                                    </select>
                                  </div>
                                  <div>
                                    <label style={{ display: 'block', fontSize: '0.7rem' }}>Ref Col</label>
                                    <select id={`sheet-fk-refcol-${idx}`} style={{ width: '100%', fontSize: '0.75rem', padding: '0.25rem' }}>
                                      <option value="">Select...</option>
                                    </select>
                                  </div>
                                  <button
                                    type="button"
                                    style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem', background: '#4CAF50', color: 'white', border: 'none', borderRadius: '3px', cursor: 'pointer' }}
                                    onClick={() => {
                                      const colSelect = document.getElementById(`sheet-fk-col-${idx}`) as HTMLSelectElement
                                      const tableSelect = document.getElementById(`sheet-fk-table-${idx}`) as HTMLSelectElement
                                      const refColSelect = document.getElementById(`sheet-fk-refcol-${idx}`) as HTMLSelectElement
                                      
                                      if (colSelect.value && tableSelect.value && refColSelect.value) {
                                        const targets = getPotentialTargets(idx)
                                        const target = targets.find(t => t.id === tableSelect.value)
                                        
                                        addSheetRelationship(idx, {
                                          foreignKey: colSelect.value,
                                          referencedTable: tableSelect.value,
                                          referencedColumn: refColSelect.value,
                                          type: 'many-to-one',
                                          referencedTableDisplayName: target?.displayName || tableSelect.value
                                        })
                                        
                                        // Reset
                                        colSelect.value = ''
                                        tableSelect.value = ''
                                        refColSelect.innerHTML = '<option value="">Select...</option>'
                                      }
                                    }}
                                  >
                                    Add
                                  </button>
                                </div>
                              </details>
                            </div>
                            
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
                    ))}
                  </div>
                </div>
              )}

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
                        {previewData.listSuggestions.map((suggestion: any, idx: number) => {
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
                disabled={uploading || (!previewData && !spreadsheetPreview) || (importMode === 'file' && !selectedFile) || (importMode === 'url' && !fileUrl)}
                style={{
                  padding: '0.75rem 1.5rem',
                  background: uploading ? '#ccc' : '#4CAF50',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: uploading ? 'not-allowed' : 'pointer'
                }}
              >
                {uploading 
                  ? 'Adding...' 
                  : (isSpreadsheet && sheetConfigs.filter(s => s.selected).length > 1 
                      ? `Add ${sheetConfigs.filter(s => s.selected).length} Tables` 
                      : 'Add Table')}
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
    </div>
  )
}

export default DatasetManage

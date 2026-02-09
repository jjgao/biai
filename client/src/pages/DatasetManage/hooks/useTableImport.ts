import { useState, useEffect, useCallback } from 'react'
import api from '../../../services/api'
import type {
  Dataset,
  Relationship,
  SpreadsheetPreview,
  SheetImportConfig,
  PreviewData,
  PotentialTarget
} from '../types'

interface UseTableImportOptions {
  datasetId: string | undefined
  dataset: Dataset | null
  onImportSuccess: () => Promise<any>
}

export function useTableImport({ datasetId, dataset, onImportSuccess }: UseTableImportOptions) {
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
  const [wasDelimiterDetected, setWasDelimiterDetected] = useState(false)
  const [detectedDelimiterName, setDetectedDelimiterName] = useState<string>('')

  // Preview state
  const [previewData, setPreviewData] = useState<PreviewData | null>(null)
  const [loadingPreview, setLoadingPreview] = useState(false)
  const [selectedPrimaryKey, setSelectedPrimaryKey] = useState('')
  const [confirmedRelationships, setConfirmedRelationships] = useState<any[]>([])
  const [selectedListColumns, setSelectedListColumns] = useState<Map<string, 'python' | 'json'>>(new Map())

  // Import configuration
  const [importTarget, setImportTarget] = useState<'new' | 'existing'>('new')
  const [targetTableId, setTargetTableId] = useState('')
  const [importModeType, setImportModeType] = useState<'append' | 'replace' | 'upsert'>('append')

  // Spreadsheet specific state
  const [isSpreadsheet, setIsSpreadsheet] = useState(false)
  const [spreadsheetPreview, setSpreadsheetPreview] = useState<SpreadsheetPreview | null>(null)
  const [sheetConfigs, setSheetConfigs] = useState<SheetImportConfig[]>([])

  const loadSpreadsheetPreview = useCallback(async (file: File | null, url: string | null) => {
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
      const response = await api.post(`/datasets/${datasetId}/spreadsheets/preview`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      })
      setSpreadsheetPreview(response.data.preview)
      setPreviewData(null)
      setSheetConfigs(response.data.preview.sheets.map((sheet: any) => ({
        sheetName: sheet.name,
        tableName: sheet.name.replace(/[^a-z0-9_]/gi, '_').toLowerCase(),
        displayName: sheet.name,
        selected: sheet.rowCount > 1,
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
  }, [datasetId])

  const loadPreview = useCallback(async (file: File | null, url: string | null) => {
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
      const response = await api.post(`/datasets/${datasetId}/tables/preview`, formData, {
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

        const delimiterNames: Record<string, string> = {
          ',': 'Comma',
          '\t': 'Tab',
          ';': 'Semicolon',
          '|': 'Pipe'
        }
        setDetectedDelimiterName(delimiterNames[detected] || detected)
      }

      // Auto-detect skipRows if not manually set
      if (skipRows === '0' && response.data.preview.detectedSkipRows !== undefined) {
        setSkipRows(String(response.data.preview.detectedSkipRows))
      }
    } catch (error: any) {
      console.error('Preview failed:', error)
      setPreviewData(null)
    } finally {
      setLoadingPreview(false)
    }
  }, [datasetId, skipRows, delimiter])

  // Auto-reload preview when skipRows or delimiter changes
  useEffect(() => {
    if (selectedFile || fileUrl) {
      const timer = setTimeout(() => {
        loadPreview(selectedFile, fileUrl)
      }, 500)
      return () => clearTimeout(timer)
    }
  }, [skipRows, delimiter])

  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const file = e.target.files[0]
      setSelectedFile(file)
      setWasDelimiterDetected(false)
      setDetectedDelimiterName('')

      const isSheet = file.name.match(/\.(xlsx|xls|ods)$/i)
      setIsSpreadsheet(!!isSheet)

      if (!tableName) {
        const name = file.name.replace(/\.[^/.]+$/, '').replace(/[^a-z0-9_]/gi, '_').toLowerCase()
        setTableName(name)
        setDisplayName(file.name.replace(/\.[^/.]+$/, ''))
      }

      if (isSheet) {
        setTimeout(() => loadSpreadsheetPreview(file, null), 100)
      } else {
        setTimeout(() => loadPreview(file, null), 100)
      }
    }
  }, [tableName, loadSpreadsheetPreview, loadPreview])

  const handleUrlChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const url = e.target.value
    setFileUrl(url)

    const isSheet = url.split('?')[0].match(/\.(xlsx|xls|ods)$/i)
    setIsSpreadsheet(!!isSheet)

    if (!tableName && url) {
      const urlPath = url.split('?')[0]
      const filename = urlPath.substring(urlPath.lastIndexOf('/') + 1)
      const name = filename.replace(/\.[^/.]+$/, '').replace(/[^a-z0-9_]/gi, '_').toLowerCase()
      setTableName(name)
      setDisplayName(filename.replace(/\.[^/.]+$/, ''))
    }
  }, [tableName])

  const handleSpreadsheetImport = useCallback(async () => {
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
      targetTableId: s.targetTableId && s.targetTableId.startsWith('PENDING:') ? '' : s.targetTableId
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
      await api.post(`/datasets/${datasetId}/spreadsheets/import`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 600000
      })
      setShowAddTable(false)
      setSelectedFile(null)
      setFileUrl('')
      setIsSpreadsheet(false)
      setSpreadsheetPreview(null)
      await onImportSuccess()
    } catch (error: any) {
      console.error('Spreadsheet import failed:', error)
      alert('Spreadsheet import failed: ' + (error.response?.data?.message || error.message))
    } finally {
      setUploading(false)
    }
  }, [datasetId, importMode, selectedFile, fileUrl, sheetConfigs, onImportSuccess])

  const handleAddTable = useCallback(async (e: React.FormEvent) => {
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
    formData.append('delimiter', delimiter)

    const finalPrimaryKey = selectedPrimaryKey || primaryKey
    if (finalPrimaryKey) formData.append('primaryKey', finalPrimaryKey)

    if (confirmedRelationships.length > 0) {
      const relationships = confirmedRelationships.map((rel: any) => ({
        foreignKey: rel.foreignKey,
        referenced_table: rel.referencedTable,
        referenced_column: rel.referenced_column
      }))
      formData.append('relationships', JSON.stringify(relationships))
    }

    if (selectedListColumns.size > 0) {
      const listColumnsObj = Object.fromEntries(selectedListColumns)
      formData.append('listColumns', JSON.stringify(listColumnsObj))
    }

    try {
      setUploading(true)
      await api.post(`/datasets/${datasetId}/tables`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 600000
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
      await onImportSuccess()
    } catch (error: any) {
      console.error('Add table failed:', error)
      alert('Add table failed: ' + (error.response?.data?.message || error.message))
    } finally {
      setUploading(false)
    }
  }, [
    isSpreadsheet, handleSpreadsheetImport, importMode, selectedFile, fileUrl,
    importTarget, tableName, targetTableId, importModeType, dataset,
    displayName, skipRows, delimiter, selectedPrimaryKey, primaryKey,
    confirmedRelationships, selectedListColumns, datasetId, onImportSuccess
  ])

  const removeSheetRelationship = useCallback((sheetIdx: number, relIdx: number) => {
    const newConfigs = [...sheetConfigs]
    newConfigs[sheetIdx].relationships = newConfigs[sheetIdx].relationships.filter((_, i) => i !== relIdx)
    setSheetConfigs(newConfigs)
  }, [sheetConfigs])

  const addSheetRelationship = useCallback((sheetIdx: number, rel: Relationship) => {
    const newConfigs = [...sheetConfigs]
    newConfigs[sheetIdx].relationships.push(rel)
    setSheetConfigs(newConfigs)
  }, [sheetConfigs])

  const getPotentialTargets = useCallback((currentSheetIdx: number): PotentialTarget[] => {
    const targets: PotentialTarget[] = []
    if (dataset) {
      targets.push(...dataset.tables.map(t => ({
        id: t.id,
        name: t.name,
        displayName: t.displayName,
        columns: t.columns.map(c => c.name)
      })))
    }
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
  }, [dataset, spreadsheetPreview, sheetConfigs])

  return {
    // State
    showAddTable, setShowAddTable,
    selectedFile,
    fileUrl,
    importMode, setImportMode,
    tableName, setTableName,
    displayName, setDisplayName,
    skipRows, setSkipRows,
    delimiter, setDelimiter,
    primaryKey, setPrimaryKey,
    uploading,
    wasDelimiterDetected, setWasDelimiterDetected,
    detectedDelimiterName,
    previewData,
    loadingPreview,
    selectedPrimaryKey, setSelectedPrimaryKey,
    confirmedRelationships, setConfirmedRelationships,
    selectedListColumns, setSelectedListColumns,
    importTarget, setImportTarget,
    targetTableId, setTargetTableId,
    importModeType, setImportModeType,
    isSpreadsheet,
    spreadsheetPreview,
    sheetConfigs, setSheetConfigs,

    // Actions
    handleFileSelect,
    handleUrlChange,
    loadPreview,
    loadSpreadsheetPreview,
    handleAddTable,
    handleSpreadsheetImport,
    removeSheetRelationship,
    addSheetRelationship,
    getPotentialTargets
  }
}

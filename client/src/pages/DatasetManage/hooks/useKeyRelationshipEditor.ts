import { useState, useCallback } from 'react'
import api from '../../../services/api'
import type { Dataset, Table, Relationship, ColumnMetadata, RelationshipFormState } from '../types'
import { hydrateRelationships } from '../utils'

interface UseKeyRelationshipEditorOptions {
  datasetId: string | undefined
  dataset: Dataset | null
  fetchDataset: (withLoading?: boolean) => Promise<Dataset | null>
  fetchTableColumns: (tableId: string) => Promise<ColumnMetadata[]>
}

export function useKeyRelationshipEditor({
  datasetId,
  dataset,
  fetchDataset,
  fetchTableColumns
}: UseKeyRelationshipEditorOptions) {
  const [showKeyEditor, setShowKeyEditor] = useState(false)
  const [keyEditorTableId, setKeyEditorTableId] = useState<string | null>(null)
  const [keyEditorColumns, setKeyEditorColumns] = useState<ColumnMetadata[]>([])
  const [keyEditorLoading, setKeyEditorLoading] = useState(false)
  const [primaryKeySelection, setPrimaryKeySelection] = useState('')
  const [initialPrimaryKeySelection, setInitialPrimaryKeySelection] = useState('')
  const [tableRelationships, setTableRelationships] = useState<Relationship[]>([])
  const [relationshipForm, setRelationshipForm] = useState<RelationshipFormState>({
    foreignKey: '',
    referencedTableId: '',
    referencedColumn: ''
  })
  const [referencedColumnsCache, setReferencedColumnsCache] = useState<Record<string, ColumnMetadata[]>>({})
  const [referencedColumnsLoading, setReferencedColumnsLoading] = useState(false)
  const [relationshipSaving, setRelationshipSaving] = useState(false)
  const [primaryKeySaving, setPrimaryKeySaving] = useState(false)

  const openKeyEditor = useCallback(async (table: Table) => {
    setKeyEditorTableId(table.id)
    const initialPk = table.primaryKey || ''
    setPrimaryKeySelection(initialPk)
    setInitialPrimaryKeySelection(initialPk)
    setRelationshipForm({ foreignKey: '', referencedTableId: '', referencedColumn: '' })
    setReferencedColumnsCache({})
    setTableRelationships(hydrateRelationships(dataset, table.relationships || []))
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
  }, [dataset, fetchTableColumns])

  const closeKeyEditor = useCallback(() => {
    setShowKeyEditor(false)
    setKeyEditorTableId(null)
    setKeyEditorColumns([])
    setInitialPrimaryKeySelection('')
    setTableRelationships([])
    setRelationshipForm({ foreignKey: '', referencedTableId: '', referencedColumn: '' })
    setReferencedColumnsCache({})
  }, [])

  const ensureReferencedColumns = useCallback(async (tableId: string) => {
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
  }, [referencedColumnsCache, fetchTableColumns])

  const handleSavePrimaryKey = useCallback(async () => {
    if (!keyEditorTableId) return
    if (primaryKeySelection === initialPrimaryKeySelection) return
    try {
      setPrimaryKeySaving(true)
      await api.patch(`/datasets/${datasetId}/tables/${keyEditorTableId}/primary-key`, {
        primaryKey: primaryKeySelection || null
      })
      const updated = await fetchDataset(false)
      if (updated) {
        const table = updated.tables.find(t => t.id === keyEditorTableId)
        if (table) {
          setTableRelationships(hydrateRelationships(updated, table.relationships || []))
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
  }, [datasetId, keyEditorTableId, primaryKeySelection, initialPrimaryKeySelection, fetchDataset])

  const handleAddRelationship = useCallback(async () => {
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
      await api.post(`/datasets/${datasetId}/tables/${keyEditorTableId}/relationships`, {
        foreignKey,
        referencedTableId,
        referencedTable: referencedTableName,
        referencedColumn
      })
      const updated = await fetchDataset(false)
      if (updated) {
        const table = updated.tables.find(t => t.id === keyEditorTableId)
        if (table) {
          setTableRelationships(hydrateRelationships(updated, table.relationships || []))
        }
      }
      setRelationshipForm({ foreignKey: '', referencedTableId: '', referencedColumn: '' })
    } catch (error) {
      console.error('Failed to add relationship:', error)
      alert('Failed to add relationship')
    } finally {
      setRelationshipSaving(false)
    }
  }, [datasetId, keyEditorTableId, relationshipForm, dataset, fetchDataset])

  const handleDeleteRelationship = useCallback(async (rel: Relationship) => {
    if (!keyEditorTableId) return
    try {
      setRelationshipSaving(true)
      await api.delete(`/datasets/${datasetId}/tables/${keyEditorTableId}/relationships`, {
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
          setTableRelationships(hydrateRelationships(updated, table.relationships || []))
        }
      }
    } catch (error) {
      console.error('Failed to delete relationship:', error)
      alert('Failed to delete relationship')
    } finally {
      setRelationshipSaving(false)
    }
  }, [datasetId, keyEditorTableId, fetchDataset])

  const primaryKeySaveDisabled = primaryKeySaving || primaryKeySelection === initialPrimaryKeySelection
  const relationshipActionDisabled =
    relationshipSaving ||
    keyEditorLoading ||
    !relationshipForm.foreignKey ||
    !relationshipForm.referencedTableId ||
    !relationshipForm.referencedColumn

  return {
    showKeyEditor,
    keyEditorTableId,
    keyEditorColumns,
    keyEditorLoading,
    primaryKeySelection,
    setPrimaryKeySelection,
    tableRelationships,
    relationshipForm,
    setRelationshipForm,
    referencedColumnsCache,
    referencedColumnsLoading,
    relationshipSaving,
    primaryKeySaving,
    primaryKeySaveDisabled,
    relationshipActionDisabled,
    openKeyEditor,
    closeKeyEditor,
    ensureReferencedColumns,
    handleSavePrimaryKey,
    handleAddRelationship,
    handleDeleteRelationship
  }
}

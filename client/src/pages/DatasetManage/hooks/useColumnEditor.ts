import { useState, useCallback } from 'react'
import api from '../../../services/api'
import type { ColumnMetadata, ColumnMetadataUpdate } from '../types'

interface UseColumnEditorOptions {
  datasetId: string | undefined
}

export function useColumnEditor({ datasetId }: UseColumnEditorOptions) {
  const [showColumnEditor, setShowColumnEditor] = useState(false)
  const [editingTableId, setEditingTableId] = useState<string | null>(null)
  const [columns, setColumns] = useState<ColumnMetadata[]>([])
  const [loadingColumns, setLoadingColumns] = useState(false)

  const fetchTableColumns = useCallback(async (tableId: string) => {
    const response = await api.get(`/datasets/${datasetId}/tables/${tableId}/columns`)
    return response.data.columns as ColumnMetadata[]
  }, [datasetId])

  const loadColumns = useCallback(async (tableId: string) => {
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
  }, [fetchTableColumns])

  const updateColumnMetadata = useCallback(async (columnName: string, updates: ColumnMetadataUpdate) => {
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

      await api.patch(`/datasets/${datasetId}/tables/${editingTableId}/columns/${columnName}`, updates)
    } catch (error) {
      console.error('Failed to update column:', error)
      alert('Failed to update column metadata')
      // Reload columns on error to revert optimistic update
      await loadColumns(editingTableId)
    }
  }, [datasetId, editingTableId, loadColumns])

  return {
    showColumnEditor,
    setShowColumnEditor,
    editingTableId,
    columns,
    loadingColumns,
    loadColumns,
    updateColumnMetadata,
    fetchTableColumns
  }
}

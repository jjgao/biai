import { useState, useCallback } from 'react'
import api from '../../../services/api'
import type { Dataset } from '../types'

interface UseTableActionsOptions {
  datasetId: string | undefined
  dataset: Dataset | null
  setDataset: (dataset: Dataset) => void
  fetchDataset: () => Promise<Dataset | null>
}

export function useTableActions({ datasetId, dataset, setDataset, fetchDataset }: UseTableActionsOptions) {
  const [selectedTable, setSelectedTable] = useState<string | null>(null)
  const [tableData, setTableData] = useState<any[]>([])
  const [loadingData, setLoadingData] = useState(false)
  const [renamingTableId, setRenamingTableId] = useState<string | null>(null)
  const [renamingTableName, setRenamingTableName] = useState('')

  const loadTableData = useCallback(async (tableId: string) => {
    try {
      setLoadingData(true)
      setSelectedTable(tableId)
      const response = await api.get(`/datasets/${datasetId}/tables/${tableId}/data?limit=100`)
      setTableData(response.data.data)
    } catch (error) {
      console.error('Failed to load table data:', error)
    } finally {
      setLoadingData(false)
    }
  }, [datasetId])

  const handleRenameTable = useCallback(async (tableId: string) => {
    if (!renamingTableName.trim()) {
      alert('Table name cannot be empty')
      return
    }

    try {
      await api.patch(`/datasets/${datasetId}/tables/${tableId}`, { displayName: renamingTableName })

      if (dataset) {
        setDataset({
          ...dataset,
          tables: dataset.tables.map(t =>
            t.id === tableId ? { ...t, displayName: renamingTableName } : t
          )
        })
      }

      setRenamingTableId(null)
      setRenamingTableName('')
    } catch (error) {
      console.error('Failed to rename table:', error)
      alert('Failed to rename table')
    }
  }, [datasetId, dataset, setDataset, renamingTableName])

  const handleDeleteTable = useCallback(async (tableId: string) => {
    if (!confirm('Are you sure you want to delete this table?')) return

    try {
      await api.delete(`/datasets/${datasetId}/tables/${tableId}`)
      await fetchDataset()
      if (selectedTable === tableId) {
        setSelectedTable(null)
        setTableData([])
      }
    } catch (error) {
      console.error('Delete table failed:', error)
      alert('Failed to delete table')
    }
  }, [datasetId, fetchDataset, selectedTable])

  return {
    selectedTable,
    tableData,
    loadingData,
    renamingTableId,
    setRenamingTableId,
    renamingTableName,
    setRenamingTableName,
    loadTableData,
    handleRenameTable,
    handleDeleteTable
  }
}

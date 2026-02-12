import { useState, useEffect, useCallback } from 'react'
import api from '../../../services/api'
import type { Dataset } from '../types'
import { normalizeDataset } from '../utils'

export function useDatasetManager(id: string | undefined) {
  const [dataset, setDataset] = useState<Dataset | null>(null)
  const [loading, setLoading] = useState(true)

  const fetchDataset = useCallback(async (withLoading: boolean = true) => {
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
  }, [id])

  const deleteDataset = useCallback(async () => {
    if (!confirm('Are you sure you want to delete this dataset and all its tables?')) return false
    try {
      await api.delete(`/datasets/${id}`)
      return true
    } catch (error) {
      console.error('Delete failed:', error)
      alert('Failed to delete dataset')
      return false
    }
  }, [id])

  useEffect(() => {
    fetchDataset()
  }, [fetchDataset])

  return {
    dataset,
    setDataset,
    loading,
    fetchDataset,
    deleteDataset
  }
}

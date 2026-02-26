import { useState, useEffect, useRef, useCallback } from 'react'
import api from '../../../services/api'
import type { SavedDashboard } from '../types'

// Re-export from shared so existing imports continue to work
export type { DashboardChart } from 'shared'
import type { DashboardChart } from 'shared'

interface UseDashboardArgs {
  identifier: string | undefined
}

export function useDashboard({ identifier }: UseDashboardArgs) {
  // Dashboard chart list
  const [dashboardCharts, setDashboardCharts] = useState<DashboardChart[]>([])

  // Visibility tracking for lazy loading
  const [visibleDashboardKeys, setVisibleDashboardKeys] = useState<Record<string, boolean>>({})
  const dashboardObserverRef = useRef<IntersectionObserver | null>(null)
  const dashboardCardRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const dashboardElementKeyMap = useRef<Map<Element, string>>(new Map())
  const intersectionObserverAvailable = typeof window !== 'undefined' && 'IntersectionObserver' in window

  // Saved dashboards state
  const [savedDashboards, setSavedDashboards] = useState<SavedDashboard[]>([])
  const [activeDashboardId, setActiveDashboardId] = useState<string | null>(null) // null = "Most Recent"
  const [showSaveDashboardDialog, setShowSaveDashboardDialog] = useState(false)
  const [showLoadDashboardDialog, setShowLoadDashboardDialog] = useState(false)
  const [showManageDashboardsDialog, setShowManageDashboardsDialog] = useState(false)
  const [newDashboardName, setNewDashboardName] = useState('')
  const [editingDashboardId, setEditingDashboardId] = useState<string | null>(null)
  const [_editingDashboardName, setEditingDashboardName] = useState('')

  // Initialization guards
  const dashboardInitialized = useRef(false)
  const savedDashboardsInitialized = useRef(false)

  const normalizeDashboardCharts = (charts: Array<{ tableName: string; columnName: string; compareColumn?: string; countByTarget?: string | null; addedAt: string }>) => {
    return charts.map(chart => ({
      tableName: chart.tableName,
      columnName: chart.columnName,
      compareColumn: chart.compareColumn,
      countByTarget: chart.countByTarget ?? null,
      addedAt: chart.addedAt || new Date().toISOString()
    }))
  }

  const getDashboardChartKey = (chart: Pick<DashboardChart, 'tableName' | 'columnName' | 'countByTarget'>) =>
    `${chart.tableName}:${chart.columnName}:${chart.countByTarget ?? 'rows'}`

  const registerDashboardCard = useCallback(
    (key: string) => (node: HTMLDivElement | null) => {
      const observer = dashboardObserverRef.current
      const prevNode = dashboardCardRefs.current[key]
      if (prevNode) {
        if (observer) {
          observer.unobserve(prevNode)
        }
        dashboardElementKeyMap.current.delete(prevNode)
      }
      if (!node) {
        dashboardCardRefs.current[key] = null
        return
      }
      dashboardCardRefs.current[key] = node
      if (observer) {
        dashboardElementKeyMap.current.set(node, key)
        observer.observe(node)
      }
    },
    []
  )

  // Saved dashboard management
  const saveDashboard = async (name: string) => {
    const newDashboard: SavedDashboard = {
      id: `dashboard_${Date.now()}`,
      name,
      charts: [...dashboardCharts],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }

    try {
      await api.post(`/datasets/${identifier}/dashboards`, {
        dashboard_id: newDashboard.id,
        dashboard_name: newDashboard.name,
        charts: newDashboard.charts,
        is_most_recent: false
      })

      setSavedDashboards(prev => [...prev, newDashboard])
      setShowSaveDashboardDialog(false)
      setNewDashboardName('')
    } catch (error) {
      console.error('Failed to save dashboard:', error)
      alert('Failed to save dashboard. Please try again.')
    }
  }

  const loadDashboard = (dashboardId: string) => {
    const dashboard = savedDashboards.find(d => d.id === dashboardId)
    if (dashboard) {
      setDashboardCharts(normalizeDashboardCharts(dashboard.charts))
      setActiveDashboardId(dashboardId)
    }
  }

  const deleteDashboard = async (dashboardId: string) => {
    try {
      await api.delete(`/datasets/${identifier}/dashboards/${dashboardId}`)

      setSavedDashboards(prev => prev.filter(d => d.id !== dashboardId))
      if (activeDashboardId === dashboardId) {
        setActiveDashboardId(null)
      }
    } catch (error) {
      console.error('Failed to delete dashboard:', error)
      alert('Failed to delete dashboard. Please try again.')
    }
  }

  const renameDashboard = async (dashboardId: string, newName: string) => {
    const dashboard = savedDashboards.find(d => d.id === dashboardId)
    if (!dashboard) return

    try {
      await api.post(`/datasets/${identifier}/dashboards`, {
        dashboard_id: dashboardId,
        dashboard_name: newName,
        charts: dashboard.charts,
        is_most_recent: false
      })

      setSavedDashboards(prev => prev.map(d =>
        d.id === dashboardId
          ? { ...d, name: newName, updatedAt: new Date().toISOString() }
          : d
      ))
      setEditingDashboardId(null)
      setEditingDashboardName('')
    } catch (error) {
      console.error('Failed to rename dashboard:', error)
      alert('Failed to rename dashboard. Please try again.')
    }
  }

  // Load "Most Recent" dashboard from database on mount (only once)
  useEffect(() => {
    if (!identifier || dashboardInitialized.current) return

    const loadMostRecentDashboard = async () => {
      try {
        const response = await api.get(`/datasets/${identifier}/dashboards`)
        const dashboards = response.data.dashboards || []
        const mostRecent = dashboards.find((d: any) => d.is_most_recent)

        if (mostRecent) {
          setDashboardCharts(normalizeDashboardCharts(mostRecent.charts))
        } else {
          // Migration: check localStorage for legacy data
          const key = `dashboard_${identifier}`
          const stored = localStorage.getItem(key)
          if (stored) {
            const charts = JSON.parse(stored)
            setDashboardCharts(normalizeDashboardCharts(charts))
            if (charts.length > 0) {
              await api.post(`/datasets/${identifier}/dashboards`, {
                dashboard_id: 'most_recent',
                dashboard_name: 'Most Recent',
                charts,
                is_most_recent: true
              })
            }
            localStorage.removeItem(key)
          }
        }
      } catch (error) {
        console.error('Failed to load most recent dashboard:', error)
        try {
          const key = `dashboard_${identifier}`
          const stored = localStorage.getItem(key)
          if (stored) {
            setDashboardCharts(normalizeDashboardCharts(JSON.parse(stored)))
          }
        } catch (e) {
          console.error('Failed to load from localStorage:', e)
        }
      } finally {
        setTimeout(() => {
          dashboardInitialized.current = true
        }, 50)
      }
    }

    loadMostRecentDashboard()
  }, [identifier])

  // Save "Most Recent" dashboard to database when changed (only after initial load)
  useEffect(() => {
    if (!dashboardInitialized.current || !identifier) return

    const saveMostRecentDashboard = async () => {
      try {
        await api.post(`/datasets/${identifier}/dashboards`, {
          dashboard_id: 'most_recent',
          dashboard_name: 'Most Recent',
          charts: dashboardCharts,
          is_most_recent: true
        })
      } catch (error) {
        console.error('Failed to save most recent dashboard:', error)
        try {
          const key = `dashboard_${identifier}`
          localStorage.setItem(key, JSON.stringify(dashboardCharts))
        } catch (e) {
          console.error('Failed to save to localStorage:', e)
        }
      }
    }

    saveMostRecentDashboard()
  }, [dashboardCharts, identifier])

  // Load saved dashboards from database on mount (only once)
  useEffect(() => {
    if (!identifier || savedDashboardsInitialized.current) return

    const loadSavedDashboards = async () => {
      try {
        const response = await api.get(`/datasets/${identifier}/dashboards`)
        const dashboards = response.data.dashboards || []

        const savedOnly = dashboards
          .filter((d: any) => !d.is_most_recent)
          .map((d: any) => ({
            id: d.dashboard_id,
            name: d.dashboard_name,
            charts: d.charts,
            createdAt: d.created_at,
            updatedAt: d.updated_at
          }))

        setSavedDashboards(savedOnly)

        // Migration: check localStorage for legacy data
        const key = `savedDashboards_${identifier}`
        const stored = localStorage.getItem(key)
        if (stored) {
          const localDashboards = JSON.parse(stored)

          for (const dashboard of localDashboards) {
            try {
              await api.post(`/datasets/${identifier}/dashboards`, {
                dashboard_id: dashboard.id,
                dashboard_name: dashboard.name,
                charts: dashboard.charts,
                is_most_recent: false
              })
            } catch (err) {
              console.error(`Failed to migrate dashboard ${dashboard.id}:`, err)
            }
          }

          localStorage.removeItem(key)

          const updatedResponse = await api.get(`/datasets/${identifier}/dashboards`)
          const updatedDashboards = updatedResponse.data.dashboards || []
          const updatedSavedOnly = updatedDashboards
            .filter((d: any) => !d.is_most_recent)
            .map((d: any) => ({
              id: d.dashboard_id,
              name: d.dashboard_name,
              charts: d.charts,
              createdAt: d.created_at,
              updatedAt: d.updated_at
            }))
          setSavedDashboards(updatedSavedOnly)
        }
      } catch (error) {
        console.error('Failed to load saved dashboards from database:', error)
        try {
          const key = `savedDashboards_${identifier}`
          const stored = localStorage.getItem(key)
          if (stored) {
            setSavedDashboards(JSON.parse(stored))
          }
        } catch (e) {
          console.error('Failed to load from localStorage:', e)
        }
      } finally {
        setTimeout(() => {
          savedDashboardsInitialized.current = true
        }, 50)
      }
    }

    loadSavedDashboards()
  }, [identifier])

  // IntersectionObserver for lazy-loading dashboard charts
  useEffect(() => {
    if (!intersectionObserverAvailable) return
    const observer = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        const key = dashboardElementKeyMap.current.get(entry.target)
        if (!key || !entry.isIntersecting) return
        setVisibleDashboardKeys(prev => {
          if (prev[key]) return prev
          return { ...prev, [key]: true }
        })
        observer.unobserve(entry.target)
        dashboardElementKeyMap.current.delete(entry.target)
      })
    }, { threshold: 0.1 })
    dashboardObserverRef.current = observer
    Object.entries(dashboardCardRefs.current).forEach(([key, node]) => {
      if (node) {
        dashboardElementKeyMap.current.set(node, key)
        observer.observe(node)
      }
    })
    return () => observer.disconnect()
  }, [intersectionObserverAvailable])

  // Fallback: if IntersectionObserver unavailable, mark all as visible
  useEffect(() => {
    if (intersectionObserverAvailable) return
    setVisibleDashboardKeys(prev => {
      let changed = false
      const next = { ...prev }
      dashboardCharts.forEach(chart => {
        const key = getDashboardChartKey(chart)
        if (!next[key]) {
          next[key] = true
          changed = true
        }
      })
      return changed ? next : prev
    })
  }, [dashboardCharts, intersectionObserverAvailable])

  // Cleanup visible keys when dashboard charts change
  useEffect(() => {
    setVisibleDashboardKeys(prev => {
      const allowed = new Set(dashboardCharts.map(chart => getDashboardChartKey(chart)))
      let changed = false
      const next: Record<string, boolean> = {}
      Object.entries(prev).forEach(([key, value]) => {
        if (allowed.has(key)) {
          next[key] = value
        } else {
          changed = true
        }
      })
      return changed ? next : prev
    })
  }, [dashboardCharts])

  return {
    // Dashboard chart state
    dashboardCharts,
    setDashboardCharts,
    visibleDashboardKeys,

    // Saved dashboards
    savedDashboards,
    activeDashboardId,
    setActiveDashboardId,
    showSaveDashboardDialog,
    setShowSaveDashboardDialog,
    showLoadDashboardDialog,
    setShowLoadDashboardDialog,
    showManageDashboardsDialog,
    setShowManageDashboardsDialog,
    newDashboardName,
    setNewDashboardName,
    editingDashboardId,
    setEditingDashboardId,
    setEditingDashboardName,

    // Functions
    getDashboardChartKey,
    registerDashboardCard,
    saveDashboard,
    loadDashboard,
    deleteDashboard,
    renameDashboard,
  }
}

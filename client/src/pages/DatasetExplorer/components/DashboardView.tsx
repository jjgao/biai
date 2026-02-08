import React from 'react'
import { ROW_COUNT_KEY } from '../../../utils/filterHelpers'
import type { DashboardChart } from '../hooks/useDashboard'
import type { SavedDashboard, ColumnAggregation } from '../types'
import { ChartHeader } from './ChartHeader'
import { getChartByType } from './renderChartByType'

interface DashboardViewProps {
  dashboardCharts: DashboardChart[]
  setDashboardCharts: React.Dispatch<React.SetStateAction<DashboardChart[]>>
  savedDashboards: SavedDashboard[]
  activeDashboardId: string | null
  setActiveDashboardId: (id: string | null) => void
  setShowLoadDashboardDialog: (value: boolean) => void
  setShowSaveDashboardDialog: (value: boolean) => void
  setShowManageDashboardsDialog: (value: boolean) => void
  getDashboardChartKey: (chart: DashboardChart) => string
  registerDashboardCard: (key: string) => (node: HTMLDivElement | null) => void
  getAggregation: (tableName: string, columnName: string, cacheKey: string) => ColumnAggregation | undefined
  getTableColor: (tableName: string) => string
  getDisplayTitle: (tableName: string, columnName: string) => string
  getColumnMetadata: (tableName: string, columnName: string) => { display_type?: string } | undefined
  getViewPreference: (tableName: string, columnName: string, categoryCount: number) => string
  getSurvivalViewPreference: (tableName: string, columnName: string) => 'histogram' | 'km'
  toggleSurvivalViewPreference: (tableName: string, columnName: string) => void
  renderDashboardCountIndicator: (chartIndex: number, tableName: string, columnName: string, cacheKey: string) => React.ReactNode
}

export function DashboardView({
  dashboardCharts,
  setDashboardCharts,
  savedDashboards,
  activeDashboardId,
  setActiveDashboardId,
  setShowLoadDashboardDialog,
  setShowSaveDashboardDialog,
  setShowManageDashboardsDialog,
  getDashboardChartKey,
  registerDashboardCard,
  getAggregation,
  getTableColor,
  getDisplayTitle,
  getColumnMetadata,
  getViewPreference,
  getSurvivalViewPreference,
  toggleSurvivalViewPreference,
  renderDashboardCountIndicator,
}: DashboardViewProps) {
  return (
    <div>
      {/* Dashboard Controls */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        marginBottom: '1rem',
        gap: '1rem'
      }}>
        <h3 style={{ margin: 0 }}>
          {activeDashboardId
            ? `Dashboard: ${savedDashboards.find(d => d.id === activeDashboardId)?.name || 'Unknown'}`
            : `Dashboard (${dashboardCharts.length} charts)`}
        </h3>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button
            onClick={() => setShowLoadDashboardDialog(true)}
            disabled={savedDashboards.length === 0}
            style={{
              padding: '0.5rem 1rem',
              background: savedDashboards.length > 0 ? '#4CAF50' : '#ccc',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: savedDashboards.length > 0 ? 'pointer' : 'not-allowed',
              fontSize: '0.875rem'
            }}
          >
            Load Dashboard
          </button>
          <button
            onClick={() => setShowSaveDashboardDialog(true)}
            disabled={dashboardCharts.length === 0}
            style={{
              padding: '0.5rem 1rem',
              background: dashboardCharts.length > 0 ? '#2196F3' : '#ccc',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: dashboardCharts.length > 0 ? 'pointer' : 'not-allowed',
              fontSize: '0.875rem'
            }}
          >
            Save Dashboard
          </button>
          <button
            onClick={() => setShowManageDashboardsDialog(true)}
            disabled={savedDashboards.length === 0}
            style={{
              padding: '0.5rem 1rem',
              background: savedDashboards.length > 0 ? '#FF9800' : '#ccc',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: savedDashboards.length > 0 ? 'pointer' : 'not-allowed',
              fontSize: '0.875rem'
            }}
          >
            Manage
          </button>
          <button
            onClick={() => {
              if (window.confirm('Clear all charts from dashboard?')) {
                setDashboardCharts([])
                setActiveDashboardId(null)
              }
            }}
            disabled={dashboardCharts.length === 0}
            style={{
              padding: '0.5rem 1rem',
              background: dashboardCharts.length > 0 ? '#f44336' : '#ccc',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: dashboardCharts.length > 0 ? 'pointer' : 'not-allowed',
              fontSize: '0.875rem'
            }}
          >
            Clear
          </button>
        </div>
      </div>

      {/* Dashboard Content */}
      {dashboardCharts.length === 0 ? (
        <div style={{
          background: 'white',
          padding: '3rem',
          borderRadius: '8px',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
          textAlign: 'center',
          color: '#666'
        }}>
          <div style={{ fontSize: '3rem', marginBottom: '1rem' }}>📊</div>
          <h3 style={{ margin: '0 0 0.5rem 0', color: '#333' }}>Your Dashboard is Empty</h3>
          <p style={{ margin: 0 }}>
            Click on the <strong>+ Add to Dashboard</strong> button on any chart in the table tabs to pin it here.
            {savedDashboards.length > 0 && <><br />Or use the <strong>Load Dashboard</strong> button above to load a saved dashboard.</>}
          </p>
        </div>
      ) : (
        <div>
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, 175px)',
            gridAutoRows: '175px',
            gap: '0.5rem',
            gridAutoFlow: 'dense'
          }}>
            {dashboardCharts.map((chart, chartIndex) => {
              const { tableName, columnName, countByTarget } = chart
              const overrideKey = countByTarget ? `parent:${countByTarget}` : ROW_COUNT_KEY
              const cardKey = getDashboardChartKey(chart)
              const cardRef = registerDashboardCard(cardKey)
              const aggregation = getAggregation(tableName, columnName, overrideKey)
              const tableColor = getTableColor(tableName)
              const displayTitle = getDisplayTitle(tableName, columnName)
              const indicatorNode = renderDashboardCountIndicator(chartIndex, tableName, columnName, overrideKey)
              const columnMeta = getColumnMetadata(tableName, columnName)
              const metaDisplayType = columnMeta?.display_type
              const normalizedDisplayType =
                aggregation?.normalized_display_type || aggregation?.display_type || metaDisplayType || ''

              if (!aggregation) {
                return (
                  <div
                    key={cardKey}
                    ref={cardRef}
                    data-dashboard-key={cardKey}
                    style={{
                      gridColumn: 'span 2',
                      minHeight: '175px',
                      background: 'white',
                      borderRadius: '8px',
                      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                      padding: '0.75rem',
                      display: 'flex',
                      flexDirection: 'column',
                      justifyContent: 'center',
                      alignItems: 'center',
                      border: tableColor ? `2px solid ${tableColor}15` : undefined
                    }}
                  >
                    <ChartHeader
                      title={displayTitle}
                      tooltip={`${displayTitle} is loading…`}
                      countIndicator={indicatorNode}
                    />
                    <div style={{ fontSize: '0.8rem', color: '#999', textAlign: 'center' }}>
                      Loading {displayTitle}…
                    </div>
                  </div>
                )
              }

              const result = getChartByType({
                title: displayTitle,
                tableName,
                columnName,
                tableColor,
                aggregation,
                cacheKey: overrideKey,
                normalizedDisplayType,
                metaDisplayType,
                countIndicatorOverride: indicatorNode,
                getViewPreference,
                getSurvivalViewPreference,
                toggleSurvivalViewPreference,
              })

              if (!result) return null

              return (
                <div
                  key={cardKey}
                  ref={cardRef}
                  data-dashboard-key={cardKey}
                  style={{
                    gridColumn: result.gridColumn,
                    gridRow: result.gridRow,
                  }}
                >
                  {result.element}
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

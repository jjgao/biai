import { useState, useEffect, useRef } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import { ROW_COUNT_KEY, rangeKey } from '../../utils/filterHelpers'
import { useFilterPresets } from './hooks/useFilterPresets'
import { useViewPreferences } from './hooks/useViewPreferences'
import { useDashboard } from './hooks/useDashboard'
import { useFilterState } from './hooks/useFilterState'
import { useCountBy } from './hooks/useCountBy'
import { useBivariate } from './hooks/useBivariate'
import { useDatasetLoader } from './hooks/useDatasetLoader'
import { ChartProvider } from './components/ChartContext'
import { ActiveFilters } from './components/ActiveFilters'
import { ChartSettingsMenu } from './components/ChartSettingsMenu'
import { SavedFiltersBar } from './components/SavedFiltersBar'
import { PresetDialogs } from './components/PresetDialogs'
import { DashboardDialogs } from './components/DashboardDialogs'
import { DatasetHeader } from './components/DatasetHeader'
import { TabBar } from './components/TabBar'
import { DashboardView } from './components/DashboardView'
import { TableSection } from './components/TableSection'
import { CountIndicator } from './components/CountIndicator'
import { DASHBOARD_SCOPE_KEY } from './types'
import {
  targetFromCacheKey,
  normalizeFilterValue,
  formatRangeValue,
  metricsMatch,
  serializeFilters,
  serializeCountBySelections,
  normalizeCountBySelections,
  getDisplayHistogram,
} from './utils'

function DatasetExplorer() {
  const { id, database } = useParams()
  const navigate = useNavigate()
  const location = useLocation()

  const isDatabaseMode = !!database
  const identifier = database || id

  // Tab navigation state
  const [activeTab, setActiveTab] = useState<string | null>(null)
  const isUpdatingURL = useRef(false)

  // ── Extracted hooks ───────────────────────────────────────────────

  const filterState = useFilterState({ identifier })

  const countBy = useCountBy({ identifier })

  const bivariate = useBivariate({ identifier })

  const {
    dashboardCharts, setDashboardCharts,
    visibleDashboardKeys,
    savedDashboards,
    activeDashboardId, setActiveDashboardId,
    showSaveDashboardDialog, setShowSaveDashboardDialog,
    showLoadDashboardDialog, setShowLoadDashboardDialog,
    showManageDashboardsDialog, setShowManageDashboardsDialog,
    newDashboardName, setNewDashboardName,
    editingDashboardId, setEditingDashboardId,
    setEditingDashboardName,
    getDashboardChartKey, registerDashboardCard,
    saveDashboard, loadDashboard, deleteDashboard, renameDashboard,
  } = useDashboard({ identifier })

  const loader = useDatasetLoader({
    id,
    database,
    identifier,
    isDatabaseMode,
    filters: filterState.filters,
    currentFiltersKey: filterState.currentFiltersKey,
    countBySelections: countBy.countBySelections,
    countByReady: countBy.countByReady,
    getCountByCacheKey: countBy.getCountByCacheKey,
    chartCountOverrides: countBy.chartCountOverrides,
    dashboardCharts,
    getDashboardChartKey,
    visibleDashboardKeys,
    previousCountByRef: countBy.previousCountByRef,
    onDatasetLoaded: () => {
      filterState.setCustomRangeInputs({})
      filterState.setRangeSelections({})
      filterState.setActiveFilterMenu(null)
      setActiveTab('dashboard')
    },
    onAncestorOptionsChanged: (options) => {
      countBy.setCountBySelections(prev => normalizeCountBySelections(prev, options))
    },
  })

  const {
    presets,
    showSavePresetDialog, setShowSavePresetDialog,
    showManagePresetsDialog, setShowManagePresetsDialog,
    showPresetsDropdown, setShowPresetsDropdown,
    presetNameInput, setPresetNameInput,
    editingPresetId, setEditingPresetId,
    savePreset, applyPreset, deletePreset, renamePreset,
    exportPresets, importPresets,
  } = useFilterPresets({
    identifier,
    filters: filterState.filters,
    countBySelections: countBy.countBySelections,
    setFilters: filterState.setFilters,
    setCountBySelections: countBy.setCountBySelections,
  })

  const {
    showPercentageLabels, setShowPercentageLabels,
    showSettingsMenu, setShowSettingsMenu,
    settingsButtonRef, settingsMenuRef,
    getViewPreference, toggleViewPreference,
    getSurvivalViewPreference, toggleSurvivalViewPreference,
  } = useViewPreferences({ identifier })

  // ── URL hash sync effect ────────────────────────────────────────
  // Bridges filter and countBy state → URL hash + localStorage

  useEffect(() => {
    if (!filterState.filtersInitialized.current || !countBy.countByInitialized.current || isUpdatingURL.current) return

    const hashParts: string[] = []

    if (filterState.filters.length === 0) {
      try {
        localStorage.removeItem(`filters_${identifier}`)
      } catch (error) {
        console.error('Failed to clear filters from localStorage:', error)
      }
    } else {
      const encodedFilters = serializeFilters(filterState.filters)
      hashParts.push(`filters=${encodedFilters}`)
      filterState.saveFiltersToLocalStorage(filterState.filters)
    }

    if (Object.keys(countBy.countBySelections).length === 0) {
      try {
        localStorage.removeItem(`countBy_${identifier}`)
      } catch (error) {
        console.error('Failed to clear countBy selections:', error)
      }
    } else {
      const encodedCountBy = serializeCountBySelections(countBy.countBySelections)
      if (encodedCountBy) {
        hashParts.push(`countBy=${encodedCountBy}`)
      }
      try {
        localStorage.setItem(`countBy_${identifier}`, JSON.stringify(countBy.countBySelections))
      } catch (error) {
        console.error('Failed to persist countBy selections:', error)
      }
    }

    const newHash = hashParts.length > 0 ? `#${hashParts.join('&')}` : ''
    const newURL = `${location.pathname}${location.search}${newHash}`

    if (newHash !== location.hash) {
      isUpdatingURL.current = true
      navigate(newURL, { replace: true })
      setTimeout(() => {
        isUpdatingURL.current = false
      }, 0)
    }
  }, [filterState.filters, countBy.countBySelections, location.pathname, location.search, location.hash, navigate, identifier])

  // ── Range init effect ───────────────────────────────────────────
  // Initializes custom range inputs when a filter menu opens (bridges filterState + loader)

  useEffect(() => {
    if (!filterState.activeFilterMenu) return
    const { tableName, columnName, countKey } = filterState.activeFilterMenu
    const key = rangeKey(tableName, columnName, countKey)
    const baselineAgg = loader.getBaselineAggregation(tableName, columnName)
    if (!baselineAgg || baselineAgg.display_type !== 'numeric') return
    const stats = baselineAgg.numeric_stats
    if (!stats) return

    const defaultMin = stats.min !== null ? String(stats.min) : ''
    const defaultMax = stats.max !== null ? String(stats.max) : ''

    const selectedRanges = filterState.rangeSelections[key] ?? []
    const singleRange = selectedRanges.length === 1 ? selectedRanges[0] : null

    const nextMin = singleRange ? String(singleRange.start) : defaultMin
    const nextMax = singleRange ? String(singleRange.end) : defaultMax

    filterState.setCustomRangeInputs(prev => {
      const current = prev[key]
      if (current && current.min === nextMin && current.max === nextMax) {
        return prev
      }
      return { ...prev, [key]: { min: nextMin, max: nextMax } }
    })
  }, [filterState.activeFilterMenu, loader.baselineAggregations, filterState.rangeSelections])

  // ── Cross-cutting bridge functions ──────────────────────────────

  const getCountByLabelFromCacheKey = (_tableName: string, cacheKey: string): string => {
    if (cacheKey.startsWith('parent:')) {
      const target = cacheKey.slice('parent:'.length)
      return loader.getTableDisplayNameByName(target) || target
    }
    return 'Rows'
  }

  const getCountByOptions = (tableName: string) => [
    {
      value: ROW_COUNT_KEY,
      label: loader.getTableDisplayNameByName(tableName) || tableName
    },
    ...(loader.ancestorOptions[tableName] || []).map(option => ({
      value: option.key,
      label: loader.getTableDisplayNameByName(option.targetTable) || option.targetTable
    }))
  ]

  const getCountIndicatorColor = (tableName: string, _cacheKey: string): string =>
    loader.getTableColor(tableName)

  const getCountByTableColor = (_tableName: string, cacheKey: string): string | null => {
    const target = targetFromCacheKey(cacheKey)
    return target ? loader.getTableColor(target) : null
  }

  // ── Dashboard bridge functions ──────────────────────────────────

  const isOnDashboard = (tableName: string, columnName: string): boolean => {
    const cacheKey = countBy.getEffectiveCacheKeyForChart(tableName, columnName)
    const target = targetFromCacheKey(cacheKey)
    return dashboardCharts.some(chart =>
      chart.tableName === tableName &&
      chart.columnName === columnName &&
      chart.countByTarget === target
    )
  }

  const toggleDashboard = (tableName: string, columnName: string) => {
    const cacheKey = countBy.getEffectiveCacheKeyForChart(tableName, columnName)
    const target = targetFromCacheKey(cacheKey)
    const compareColumn = bivariate.getBivariateSelection(tableName, columnName)
    if (isOnDashboard(tableName, columnName)) {
      setDashboardCharts(prev =>
        prev.filter(chart =>
          !(chart.tableName === tableName && chart.columnName === columnName && chart.countByTarget === target)
        ))
    } else {
      setDashboardCharts(prev => [...prev, { tableName, columnName, compareColumn, countByTarget: target, addedAt: new Date().toISOString() }])
      loader.ensureAggregationForCacheKey(tableName, cacheKey)
    }
  }

  const addAllChartsToTable = (tableName: string) => {
    const tableAggregations = loader.getAggregationsForTable(tableName)
    const tableMetadata = loader.columnMetadata[tableName]
    if (!tableAggregations || tableAggregations.length === 0) return
    if (!tableMetadata || !Array.isArray(tableMetadata)) return

    const visibleAggregations = tableAggregations.filter(agg => {
      const metadata = tableMetadata.find(m => m.column_name === agg.column_name)
      return !metadata?.is_hidden
    })

    const newCharts = visibleAggregations
      .map(agg => {
        const cacheKey = countBy.getEffectiveCacheKeyForChart(tableName, agg.column_name)
        const target = targetFromCacheKey(cacheKey)
        return {
          tableName,
          columnName: agg.column_name,
          countByTarget: target,
          addedAt: new Date().toISOString()
        }
      })
      .filter(newChart =>
        !dashboardCharts.some(chart =>
          chart.tableName === newChart.tableName &&
          chart.columnName === newChart.columnName &&
          chart.countByTarget === newChart.countByTarget
        )
      )

    if (newCharts.length > 0) {
      setDashboardCharts(prev => [...prev, ...newCharts])
      newCharts.forEach(chart => {
        const cacheKey = chart.countByTarget ? `parent:${chart.countByTarget}` : ROW_COUNT_KEY
        loader.ensureAggregationForCacheKey(chart.tableName, cacheKey)
      })
    }
  }

  // ── Count-by override handlers ──────────────────────────────────

  const handleChartCountOverrideChange = (tableName: string, columnName: string, value: string) => {
    const defaultKey = countBy.getCountByCacheKey(tableName)
    if (value === defaultKey) {
      countBy.setChartOverrideForChart(tableName, columnName)
    } else {
      countBy.setChartOverrideForChart(tableName, columnName, value)
    }
    loader.ensureAggregationForCacheKey(tableName, value)
  }

  const handleDashboardChartCountChange = (chartIndex: number, tableName: string, value: string) => {
    const nextTarget = value.startsWith('parent:') ? value.slice('parent:'.length) : null
    setDashboardCharts(prev =>
      prev.map((chart, idx) =>
        idx === chartIndex ? { ...chart, countByTarget: nextTarget } : chart
      )
    )
    loader.ensureAggregationForCacheKey(tableName, value)
  }

  // ── Count indicator wrappers ────────────────────────────────────

  const renderDashboardCountIndicator = (
    chartIndex: number,
    tableName: string,
    columnName: string,
    cacheKey: string
  ) => (
    <CountIndicator
      menuKey={`${DASHBOARD_SCOPE_KEY}:${chartIndex}`}
      indicatorColor={getCountIndicatorColor(tableName, cacheKey)}
      borderColor={getCountByTableColor(tableName, cacheKey)}
      label={getCountByLabelFromCacheKey(tableName, cacheKey)}
      options={getCountByOptions(tableName)}
      currentValue={cacheKey}
      buttonLabel={`Change count-by for dashboard chart ${tableName}.${columnName}`}
      onSelect={value => handleDashboardChartCountChange(chartIndex, tableName, value)}
    />
  )

  const renderTabCountIndicator = (tableName: string, cacheKey: string) => (
    <CountIndicator
      menuKey={`tab:${tableName}`}
      indicatorColor={getCountIndicatorColor(tableName, cacheKey)}
      borderColor={getCountByTableColor(tableName, cacheKey)}
      label={getCountByLabelFromCacheKey(tableName, cacheKey)}
      options={getCountByOptions(tableName)}
      currentValue={cacheKey}
      buttonLabel={`Change count-by for ${tableName}`}
      onSelect={value => countBy.handleCountByChange(tableName, value)}
      size="large"
    />
  )

  // ── ChartContext assembly ───────────────────────────────────────

  const chartContextValue = {
    getEffectiveCacheKeyForChart: countBy.getEffectiveCacheKeyForChart,
    getAggregation: loader.getAggregation,
    getBaselineAggregation: loader.getBaselineAggregation,
    getMetricLabels: loader.getMetricLabels,
    metricsMatch,
    getColumnMetadata: loader.getColumnMetadata,
    getTableDisplayNameByName: loader.getTableDisplayNameByName,
    formatMetricPath: loader.formatMetricPath,
    getCountByOptions,
    getCountByCacheKey: countBy.getCountByCacheKey,
    handleCountByChange: countBy.handleCountByChange,
    handleChartCountOverrideChange,
    activeCountMenuKey: countBy.activeCountMenuKey,
    setActiveCountMenuKey: countBy.setActiveCountMenuKey,
    getTableColor: loader.getTableColor,
    getCountIndicatorColor,
    getCountByTableColor,
    getCountByLabelFromCacheKey,
    activeFilterMenu: filterState.activeFilterMenu,
    setActiveFilterMenu: filterState.setActiveFilterMenu,
    filters: filterState.filters,
    setFilters: filterState.setFilters,
    hasColumnFilter: filterState.hasColumnFilter,
    isValueFiltered: filterState.isValueFiltered,
    normalizeFilterValue,
    toggleFilter: filterState.toggleFilter,
    clearColumnFilter: filterState.clearColumnFilter,
    removeColumnFilters: filterState.removeColumnFilters,
    rangeSelections: filterState.rangeSelections,
    customRangeInputs: filterState.customRangeInputs,
    toggleRangeFilter: filterState.toggleRangeFilter,
    isRangeFiltered: filterState.isRangeFiltered,
    handleCustomRangeChange: filterState.handleCustomRangeChange,
    applyCustomRange: filterState.applyCustomRange,
    updateColumnRanges: filterState.updateColumnRanges,
    formatRangeValue,
    getDisplayHistogram,
    showPercentageLabels,
    toggleViewPreference,
    getSurvivalViewPreference,
    toggleSurvivalViewPreference,
    findTable: (tableName: string) => loader.dataset?.tables.find(t => t.name === tableName),
    findSurvivalStatusColumn: loader.findSurvivalStatusColumn,
    ensureSurvivalCurve: loader.ensureSurvivalCurve,
    getSurvivalCurve: loader.getSurvivalCurve,
    isOnDashboard,
    toggleDashboard,
    getBivariateSelection: bivariate.getBivariateSelection,
    setBivariateSelection: bivariate.setBivariateSelection,
    getCategoricalColumns: (tableName: string) => {
      const metadata = loader.columnMetadata[tableName]
      if (!metadata) return []
      return metadata.filter(col =>
        col.display_type === 'categorical' && !col.is_hidden
      )
    },
    getBivariateData: loader.getBivariateData,
    ensureBivariateData: loader.ensureBivariateData,
  }

  // ── Render ──────────────────────────────────────────────────────

  if (loader.loading) return <p>Loading explorer...</p>
  if (loader.error) return <div role="alert" style={{ padding: '2rem', color: 'red' }}>Error: {loader.error}</div>
  if (!loader.dataset) return <p>Dataset not found</p>

  return (
    <ChartProvider value={chartContextValue}>
    <div>
      {/* Header */}
      <DatasetHeader
        name={loader.dataset.name}
        description={loader.dataset.description}
        tableCount={loader.dataset.tables.length}
        onManage={() => navigate(`/datasets/${id}/manage`)}
      />

      <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '-1rem', marginBottom: '0.5rem' }}>
        <div style={{ position: 'relative', display: 'inline-block' }}>
          <button
            ref={settingsButtonRef}
            onClick={() => setShowSettingsMenu(prev => !prev)}
            style={{
              border: 'none',
              borderRadius: '4px',
              padding: '0.3rem 0.6rem',
              background: '#ECEFF1',
              color: '#333',
              cursor: 'pointer',
              fontSize: '0.75rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.35rem'
            }}
            aria-label="Chart settings"
          >
            <span role="img" aria-hidden="true">⚙</span>
            Chart settings
          </button>
          {showSettingsMenu && (
            <ChartSettingsMenu
              showPercentageLabels={showPercentageLabels}
              setShowPercentageLabels={setShowPercentageLabels}
              onClose={() => setShowSettingsMenu(false)}
              identifier={identifier}
              menuRef={settingsMenuRef}
            />
          )}
        </div>
      </div>

      <SavedFiltersBar
        presets={presets}
        showPresetsDropdown={showPresetsDropdown}
        setShowPresetsDropdown={setShowPresetsDropdown}
        setShowManagePresetsDialog={setShowManagePresetsDialog}
      />

      <ActiveFilters
        filters={filterState.filters}
        setFilters={filterState.setFilters}
        onSaveFilter={() => setShowSavePresetDialog(true)}
        onClearFilters={filterState.clearFilters}
        getTableColor={loader.getTableColor}
        getFilterTableNameForCacheKey={filterState.getFilterTableNameForCacheKey}
        targetFromCacheKey={targetFromCacheKey}
        formatRangeValue={formatRangeValue}
        clearColumnFilter={filterState.clearColumnFilter}
        datasetTables={loader.dataset?.tables}
      />

      <PresetDialogs
        presets={presets}
        showSavePresetDialog={showSavePresetDialog}
        setShowSavePresetDialog={setShowSavePresetDialog}
        showPresetsDropdown={showPresetsDropdown}
        setShowPresetsDropdown={setShowPresetsDropdown}
        showManagePresetsDialog={showManagePresetsDialog}
        setShowManagePresetsDialog={setShowManagePresetsDialog}
        presetNameInput={presetNameInput}
        setPresetNameInput={setPresetNameInput}
        editingPresetId={editingPresetId}
        setEditingPresetId={setEditingPresetId}
        savePreset={savePreset}
        applyPreset={applyPreset}
        deletePreset={deletePreset}
        renamePreset={renamePreset}
        exportPresets={exportPresets}
        importPresets={importPresets}
      />

      <DashboardDialogs
        savedDashboards={savedDashboards}
        activeDashboardId={activeDashboardId}
        showSaveDashboardDialog={showSaveDashboardDialog}
        setShowSaveDashboardDialog={setShowSaveDashboardDialog}
        showLoadDashboardDialog={showLoadDashboardDialog}
        setShowLoadDashboardDialog={setShowLoadDashboardDialog}
        showManageDashboardsDialog={showManageDashboardsDialog}
        setShowManageDashboardsDialog={setShowManageDashboardsDialog}
        newDashboardName={newDashboardName}
        setNewDashboardName={setNewDashboardName}
        editingDashboardId={editingDashboardId}
        setEditingDashboardId={setEditingDashboardId}
        setEditingDashboardName={setEditingDashboardName}
        saveDashboard={saveDashboard}
        loadDashboard={loadDashboard}
        deleteDashboard={deleteDashboard}
        renameDashboard={renameDashboard}
      />

      <TabBar
        tables={loader.dataset.tables}
        activeTab={activeTab}
        onTabChange={setActiveTab}
        dashboardChartCount={dashboardCharts.length}
        getTableColor={loader.getTableColor}
        getTableChartCount={loader.getTableChartCount}
      />

      {/* Dashboard View */}
      {activeTab === 'dashboard' && (
        <DashboardView
          dashboardCharts={dashboardCharts}
          setDashboardCharts={setDashboardCharts}
          savedDashboards={savedDashboards}
          activeDashboardId={activeDashboardId}
          setActiveDashboardId={setActiveDashboardId}
          setShowLoadDashboardDialog={setShowLoadDashboardDialog}
          setShowSaveDashboardDialog={setShowSaveDashboardDialog}
          setShowManageDashboardsDialog={setShowManageDashboardsDialog}
          getDashboardChartKey={getDashboardChartKey}
          registerDashboardCard={registerDashboardCard}
          getAggregation={loader.getAggregation}
          getTableColor={loader.getTableColor}
          getDisplayTitle={loader.getDisplayTitle}
          getColumnMetadata={loader.getColumnMetadata}
          getViewPreference={getViewPreference}
          getSurvivalViewPreference={getSurvivalViewPreference}
          toggleSurvivalViewPreference={toggleSurvivalViewPreference}
          renderDashboardCountIndicator={renderDashboardCountIndicator}
        />
      )}

      {/* Chart Grid - Grouped by Table */}
      {loader.dataset.tables
        .filter(table => table.name === activeTab)
        .map(table => {
          const tableAggregations = loader.getAggregationsForTable(table.name)
          if (!tableAggregations) return null

          const sortedAggregations = [...tableAggregations].sort((a, b) => {
            const metaA = loader.getColumnMetadata(table.name, a.column_name)
            const metaB = loader.getColumnMetadata(table.name, b.column_name)
            const priorityA = metaA?.display_priority || 0
            const priorityB = metaB?.display_priority || 0
            return priorityB - priorityA
          })

          // Filter out hidden columns
          const visibleAggregations = sortedAggregations.filter(agg => {
            const metadata = loader.getColumnMetadata(table.name, agg.column_name)
            return !metadata?.is_hidden
          })

          if (visibleAggregations.length === 0) return null

          const tableColor = loader.getTableColor(table.name)
          const primaryAggregation = visibleAggregations[0]

          // Get baseline (unfiltered) row count for this table
          const baselineTableAggs = loader.baselineAggregations[table.name] || []
          const baselineSample = baselineTableAggs.length > 0 ? baselineTableAggs[0] : undefined
          const baselineMatches = metricsMatch(baselineSample, primaryAggregation)
          const tableRowCount = primaryAggregation?.total_rows ?? table.rowCount ?? 0
          const baselineRowCount = baselineMatches
            ? baselineSample?.total_rows ?? tableRowCount
            : null

          // Get filter counts for this table
          const effectiveFilters = loader.getAllEffectiveFilters()
          const tableFilters = effectiveFilters[table.name] || { direct: [], propagated: [] }
          const directFilterCount = tableFilters.direct.length
          const propagatedFilterCount = tableFilters.propagated.length
          const hasTableFilters = directFilterCount > 0 || propagatedFilterCount > 0

          const metricLabels = loader.getMetricLabels(primaryAggregation)
          const parentOptions = loader.ancestorOptions[table.name] || []
          const countByValue = countBy.getCountByValueForTable(table.name)

          return (
            <TableSection
              key={table.name}
              table={table}
              tables={loader.dataset!.tables}
              tableColor={tableColor}
              visibleAggregations={visibleAggregations}
              primaryAggregation={primaryAggregation}
              baselineRowCount={baselineRowCount}
              hasTableFilters={hasTableFilters}
              directFilterCount={directFilterCount}
              propagatedFilterCount={propagatedFilterCount}
              propagatedFilters={tableFilters.propagated}
              metricLabels={metricLabels}
              countByValue={countByValue}
              parentOptions={parentOptions}
              getTableColor={loader.getTableColor}
              getDisplayTitle={loader.getDisplayTitle}
              getColumnMetadata={loader.getColumnMetadata}
              getEffectiveCacheKeyForChart={countBy.getEffectiveCacheKeyForChart}
              getCountByCacheKey={countBy.getCountByCacheKey}
              getCountByLabelFromCacheKey={getCountByLabelFromCacheKey}
              getViewPreference={getViewPreference}
              getSurvivalViewPreference={getSurvivalViewPreference}
              toggleSurvivalViewPreference={toggleSurvivalViewPreference}
              addAllChartsToTable={addAllChartsToTable}
              renderTabCountIndicator={renderTabCountIndicator}
            />
          )
        })}
    </div>
    </ChartProvider>
  )
}

export default DatasetExplorer

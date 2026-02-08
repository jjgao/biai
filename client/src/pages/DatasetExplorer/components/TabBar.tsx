interface TabBarProps {
  tables: Array<{ name: string; displayName?: string }>
  activeTab: string | null
  onTabChange: (tab: string) => void
  dashboardChartCount: number
  getTableColor: (tableName: string) => string
  getTableChartCount: (tableName: string) => number
}

export function TabBar({
  tables,
  activeTab,
  onTabChange,
  dashboardChartCount,
  getTableColor,
  getTableChartCount,
}: TabBarProps) {
  return (
    <div style={{
      marginBottom: '1.5rem',
      background: 'white',
      padding: '0.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      display: 'flex',
      gap: '0.5rem',
      flexWrap: 'wrap'
    }}>
      {/* Dashboard Tab */}
      <button
        onClick={() => onTabChange('dashboard')}
        style={{
          padding: '0.75rem 1.5rem',
          background: activeTab === 'dashboard' ? '#607D8B' : 'transparent',
          color: activeTab === 'dashboard' ? 'white' : '#333',
          border: `2px solid ${activeTab === 'dashboard' ? '#607D8B' : '#E0E0E0'}`,
          borderRadius: '6px',
          cursor: 'pointer',
          fontSize: '0.875rem',
          fontWeight: activeTab === 'dashboard' ? 600 : 400,
          transition: 'all 0.2s ease',
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem'
        }}
        onMouseEnter={(e) => {
          if (activeTab !== 'dashboard') {
            e.currentTarget.style.borderColor = '#607D8B'
            e.currentTarget.style.color = '#607D8B'
          }
        }}
        onMouseLeave={(e) => {
          if (activeTab !== 'dashboard') {
            e.currentTarget.style.borderColor = '#E0E0E0'
            e.currentTarget.style.color = '#333'
          }
        }}
      >
        <div style={{
          width: '8px',
          height: '20px',
          borderRadius: '2px',
          background: activeTab === 'dashboard' ? 'white' : '#607D8B'
        }} />
        Dashboard {dashboardChartCount > 0 && `(${dashboardChartCount})`}
      </button>

      {/* Table Tabs */}
      {tables.map(table => {
        const tableColor = getTableColor(table.name)
        const isActive = activeTab === table.name
        const chartCount = getTableChartCount(table.name)

        return (
          <button
            key={table.name}
            onClick={() => onTabChange(table.name)}
            style={{
              padding: '0.75rem 1.5rem',
              background: isActive ? tableColor : 'transparent',
              color: isActive ? 'white' : '#333',
              border: `2px solid ${isActive ? tableColor : '#E0E0E0'}`,
              borderRadius: '6px',
              cursor: 'pointer',
              fontSize: '0.875rem',
              fontWeight: isActive ? 600 : 400,
              transition: 'all 0.2s ease',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem'
            }}
            onMouseEnter={(e) => {
              if (!isActive) {
                e.currentTarget.style.borderColor = tableColor
                e.currentTarget.style.color = tableColor
              }
            }}
            onMouseLeave={(e) => {
              if (!isActive) {
                e.currentTarget.style.borderColor = '#E0E0E0'
                e.currentTarget.style.color = '#333'
              }
            }}
          >
            <div style={{
              width: '8px',
              height: '20px',
              borderRadius: '2px',
              background: isActive ? 'white' : tableColor
            }} />
            {table.displayName || table.name} {chartCount > 0 && `(${chartCount})`}
          </button>
        )
      })}
    </div>
  )
}

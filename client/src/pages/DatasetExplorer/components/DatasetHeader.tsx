import SafeHtml from '../../../components/SafeHtml'

interface DatasetHeaderProps {
  name: string
  description?: string
  tableCount: number
  onManage: () => void
}

export function DatasetHeader({ name, description, tableCount, onManage }: DatasetHeaderProps) {
  return (
    <div style={{ marginBottom: '2rem', background: 'white', padding: '1.5rem', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', position: 'relative' }}>
      <button
        onClick={onManage}
        style={{
          position: 'absolute',
          top: '1.5rem',
          right: '1.5rem',
          padding: '0.5rem',
          background: '#757575',
          color: 'white',
          border: 'none',
          borderRadius: '4px',
          cursor: 'pointer',
          fontSize: '1.2rem',
          lineHeight: '1',
          width: '32px',
          height: '32px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center'
        }}
        title="Manage dataset"
      >
        ✎
      </button>

      <h2 style={{ marginTop: 0, paddingRight: '3rem' }}>{name}</h2>
      {description && (
        <SafeHtml
          html={description}
          style={{ color: '#666', margin: '0.5rem 0', display: 'block' }}
        />
      )}

      <div style={{ display: 'flex', gap: '2rem', marginTop: '1rem', fontSize: '0.875rem' }}>
        <div>
          <strong>Tables:</strong> {tableCount}
        </div>
      </div>
    </div>
  )
}

import { useState } from 'react'

interface SqlViewerModalProps {
  sql: string
  columnName: string
  onClose: () => void
}

export function SqlViewerModal({ sql, columnName, onClose }: SqlViewerModalProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(sql)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch (err) {
      console.error('Failed to copy SQL:', err)
    }
  }

  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: 'rgba(0, 0, 0, 0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000
      }}
      onClick={onClose}
    >
      <div
        style={{
          backgroundColor: 'white',
          borderRadius: '8px',
          width: '90%',
          maxWidth: '600px',
          maxHeight: '80vh',
          display: 'flex',
          flexDirection: 'column',
          boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)'
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          style={{
            padding: '16px 20px',
            borderBottom: '1px solid #e0e0e0',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center'
          }}
        >
          <h3 style={{ margin: 0, fontSize: '16px', fontWeight: 600 }}>
            SQL Query — {columnName}
          </h3>
          <button
            onClick={onClose}
            style={{
              background: 'none',
              border: 'none',
              fontSize: '20px',
              cursor: 'pointer',
              color: '#666',
              padding: '0 4px',
              lineHeight: '1'
            }}
            title="Close"
          >
            ×
          </button>
        </div>

        {/* Body */}
        <div
          style={{
            padding: '20px',
            overflowY: 'auto',
            flex: 1
          }}
        >
          <pre
            style={{
              backgroundColor: '#f5f5f5',
              padding: '12px',
              borderRadius: '4px',
              fontSize: '12px',
              fontFamily: 'Monaco, Menlo, "Courier New", monospace',
              overflowX: 'auto',
              margin: 0,
              whiteSpace: 'pre-wrap',
              overflowWrap: 'break-word'
            }}
          >
            {sql}
          </pre>
        </div>

        {/* Footer */}
        <div
          style={{
            padding: '12px 20px',
            borderTop: '1px solid #e0e0e0',
            display: 'flex',
            justifyContent: 'flex-end'
          }}
        >
          <button
            onClick={handleCopy}
            style={{
              padding: '8px 16px',
              backgroundColor: copied ? '#4caf50' : '#1976d2',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '14px',
              fontWeight: 500,
              transition: 'background-color 0.2s'
            }}
          >
            {copied ? 'Copied!' : 'Copy to Clipboard'}
          </button>
        </div>
      </div>
    </div>
  )
}

interface SqlViewButtonProps {
  sql?: string
  columnName: string
}

export function SqlViewButton({ sql, columnName }: SqlViewButtonProps) {
  const [showModal, setShowModal] = useState(false)

  if (!sql) return null

  return (
    <>
      <button
        type="button"
        onClick={(e) => { e.stopPropagation(); setShowModal(true) }}
        style={{
          width: '20px',
          height: '20px',
          borderRadius: '50%',
          backgroundColor: '#f0f0f0',
          border: 'none',
          cursor: 'pointer',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '0.65rem',
          fontWeight: 'bold',
          color: '#666',
          lineHeight: 1
        }}
        title="View SQL query"
      >
        {'<>'}
      </button>
      {showModal && (
        <SqlViewerModal
          sql={sql}
          columnName={columnName}
          onClose={() => setShowModal(false)}
        />
      )}
    </>
  )
}

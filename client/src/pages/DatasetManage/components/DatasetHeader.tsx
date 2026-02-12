import SafeHtml from '../../../components/SafeHtml'
import type { Dataset } from '../types'

interface DatasetHeaderProps {
  dataset: Dataset
  onBack: () => void
  onExplore: () => void
  onDelete: () => void
}

export default function DatasetHeader({ dataset, onBack, onExplore, onDelete }: DatasetHeaderProps) {
  return (
    <>
      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
        <button
          onClick={onBack}
          style={{
            padding: '0.5rem 1rem',
            background: '#666',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer'
          }}
        >
          ← Back
        </button>
        <button
          onClick={onExplore}
          style={{
            padding: '0.5rem 1rem',
            background: '#2196F3',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer'
          }}
        >
          📊 Explore Data
        </button>
        <button
          onClick={onDelete}
          style={{
            padding: '0.5rem 1rem',
            background: '#f44336',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            marginLeft: 'auto'
          }}
        >
          Delete Dataset
        </button>
      </div>

      <div style={{ marginBottom: '2rem', background: 'white', padding: '1.5rem', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
        <h2 style={{ marginTop: 0 }}>{dataset.name}</h2>
        {dataset.description && (
          <SafeHtml
            html={dataset.description}
            style={{ color: '#666', display: 'block', margin: '0.5rem 0 0 0' }}
          />
        )}

        {dataset.tags && dataset.tags.length > 0 && (
          <div style={{ marginTop: '1rem', display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            <strong style={{ marginRight: '0.5rem' }}>Tags:</strong>
            {dataset.tags.map((tag, i) => (
              <span
                key={i}
                style={{
                  padding: '0.25rem 0.5rem',
                  background: '#e3f2fd',
                  color: '#1976d2',
                  borderRadius: '4px',
                  fontSize: '0.875rem',
                  fontWeight: 500
                }}
              >
                {tag}
              </span>
            ))}
          </div>
        )}

        {dataset.source && (
          <div style={{ marginTop: '0.75rem', fontSize: '0.875rem', color: '#666' }}>
            <strong>Source:</strong> {dataset.source}
          </div>
        )}

        {dataset.citation && (
          <div style={{ marginTop: '0.5rem', fontSize: '0.875rem', color: '#666' }}>
            <strong>Citation:</strong> {dataset.citation}
          </div>
        )}

        {dataset.references && dataset.references.length > 0 && (
          <div style={{ marginTop: '0.75rem', fontSize: '0.875rem', color: '#666' }}>
            <strong>References:</strong>
            <ul style={{ margin: '0.5rem 0 0 1.5rem', padding: 0 }}>
              {dataset.references.map((ref, i) => (
                <li key={i} style={{ marginBottom: '0.25rem' }}>
                  {ref.startsWith('pmid:') ? (
                    <a href={`https://pubmed.ncbi.nlm.nih.gov/${ref.substring(5)}/`} target="_blank" rel="noopener noreferrer" style={{ color: '#1976d2' }}>
                      {ref}
                    </a>
                  ) : ref.startsWith('doi:') ? (
                    <a href={`https://doi.org/${ref.substring(4)}`} target="_blank" rel="noopener noreferrer" style={{ color: '#1976d2' }}>
                      {ref}
                    </a>
                  ) : (
                    ref
                  )}
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </>
  )
}

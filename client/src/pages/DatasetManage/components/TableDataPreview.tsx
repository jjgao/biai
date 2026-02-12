interface TableDataPreviewProps {
  tableData: any[]
  loadingData: boolean
}

export default function TableDataPreview({ tableData, loadingData }: TableDataPreviewProps) {
  return (
    <div style={{ marginTop: '2rem' }}>
      <h3>Table Data Preview</h3>
      {loadingData ? (
        <p>Loading data...</p>
      ) : tableData.length > 0 ? (
        <div style={{ overflowX: 'auto', background: 'white', padding: '1rem', borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem' }}>
            <thead>
              <tr style={{ borderBottom: '2px solid #ddd' }}>
                {Object.keys(tableData[0]).map((key) => (
                  <th key={key} style={{ padding: '0.5rem', textAlign: 'left', background: '#f5f5f5' }}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {tableData.slice(0, 50).map((row, idx) => (
                <tr key={idx} style={{ borderBottom: '1px solid #eee' }}>
                  {Object.values(row).map((val: any, i) => (
                    <td key={i} style={{ padding: '0.5rem' }}>{val?.toString() || '-'}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {tableData.length > 50 && (
            <p style={{ marginTop: '1rem', color: '#666', fontSize: '0.875rem' }}>
              Showing first 50 of {tableData.length} rows
            </p>
          )}
        </div>
      ) : (
        <p>No data available</p>
      )}
    </div>
  )
}

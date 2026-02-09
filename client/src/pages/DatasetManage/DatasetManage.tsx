import { useParams, useNavigate } from 'react-router-dom'
import { useDatasetManager } from './hooks/useDatasetManager'
import { useTableImport } from './hooks/useTableImport'
import { useColumnEditor } from './hooks/useColumnEditor'
import { useKeyRelationshipEditor } from './hooks/useKeyRelationshipEditor'
import { useTableActions } from './hooks/useTableActions'
import DatasetHeader from './components/DatasetHeader'
import FileSourceSelector from './components/FileSourceSelector'
import CsvImportConfig from './components/CsvImportConfig'
import SpreadsheetSheetsConfig from './components/SpreadsheetSheetsConfig'
import CsvPreviewSection from './components/CsvPreviewSection'
import TableCard from './components/TableCard'
import TableDataPreview from './components/TableDataPreview'
import ColumnEditorModal from './components/ColumnEditorModal'
import KeyRelationshipEditorModal from './components/KeyRelationshipEditorModal'

function DatasetManage() {
  const { id } = useParams()
  const navigate = useNavigate()

  // Core dataset state
  const {
    dataset, setDataset, loading, fetchDataset, deleteDataset
  } = useDatasetManager(id)

  // Table import
  const tableImport = useTableImport({
    datasetId: id,
    dataset,
    onImportSuccess: fetchDataset
  })

  // Column editor
  const columnEditor = useColumnEditor({ datasetId: id })

  // Key & relationship editor
  const keyRelEditor = useKeyRelationshipEditor({
    datasetId: id,
    dataset,
    fetchDataset,
    fetchTableColumns: columnEditor.fetchTableColumns
  })

  // Table actions (view data, rename, delete)
  const tableActions = useTableActions({
    datasetId: id,
    dataset,
    setDataset,
    fetchDataset
  })

  if (loading) return <p>Loading dataset...</p>
  if (!dataset) return <p>Dataset not found</p>

  return (
    <div>
      <DatasetHeader
        dataset={dataset}
        onBack={() => navigate(-1)}
        onExplore={() => navigate(`/datasets/${id}`)}
        onDelete={async () => {
          const deleted = await deleteDataset()
          if (deleted) navigate('/datasets')
        }}
      />

      <div style={{ marginBottom: '2rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
          <h3>Tables ({dataset.tables.length})</h3>
          <button
            onClick={() => tableImport.setShowAddTable(!tableImport.showAddTable)}
            style={{
              padding: '0.5rem 1rem',
              background: '#4CAF50',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            {tableImport.showAddTable ? 'Cancel' : '+ Add Table'}
          </button>
        </div>

        {tableImport.showAddTable && (
          <div style={{
            background: 'white',
            padding: '1.5rem',
            borderRadius: '8px',
            marginBottom: '1rem',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
          }}>
            <h4 style={{ marginTop: 0 }}>Add Table to Dataset</h4>
            <form onSubmit={tableImport.handleAddTable}>
              <FileSourceSelector
                importMode={tableImport.importMode}
                setImportMode={tableImport.setImportMode}
                fileUrl={tableImport.fileUrl}
                loadingPreview={tableImport.loadingPreview}
                onFileSelect={tableImport.handleFileSelect}
                onUrlChange={tableImport.handleUrlChange}
                onUrlLoad={() => {
                  if (tableImport.fileUrl) {
                    const isSheet = tableImport.fileUrl.split('?')[0].match(/\.(xlsx|xls|ods)$/i)
                    if (isSheet) tableImport.loadSpreadsheetPreview(null, tableImport.fileUrl)
                    else tableImport.loadPreview(null, tableImport.fileUrl)
                  }
                }}
              />

              <CsvImportConfig
                dataset={dataset}
                importTarget={tableImport.importTarget}
                setImportTarget={tableImport.setImportTarget}
                targetTableId={tableImport.targetTableId}
                setTargetTableId={tableImport.setTargetTableId}
                importModeType={tableImport.importModeType}
                setImportModeType={tableImport.setImportModeType}
                tableName={tableImport.tableName}
                setTableName={tableImport.setTableName}
                displayName={tableImport.displayName}
                setDisplayName={tableImport.setDisplayName}
                skipRows={tableImport.skipRows}
                setSkipRows={tableImport.setSkipRows}
                delimiter={tableImport.delimiter}
                setDelimiter={tableImport.setDelimiter}
                wasDelimiterDetected={tableImport.wasDelimiterDetected}
                setWasDelimiterDetected={tableImport.setWasDelimiterDetected}
                detectedDelimiterName={tableImport.detectedDelimiterName}
                primaryKey={tableImport.primaryKey}
                setPrimaryKey={tableImport.setPrimaryKey}
                isSpreadsheet={tableImport.isSpreadsheet}
              />

              {tableImport.isSpreadsheet && tableImport.spreadsheetPreview && (
                <SpreadsheetSheetsConfig
                  dataset={dataset}
                  spreadsheetPreview={tableImport.spreadsheetPreview}
                  sheetConfigs={tableImport.sheetConfigs}
                  setSheetConfigs={tableImport.setSheetConfigs}
                  removeSheetRelationship={tableImport.removeSheetRelationship}
                  addSheetRelationship={tableImport.addSheetRelationship}
                  getPotentialTargets={tableImport.getPotentialTargets}
                />
              )}

              {!tableImport.isSpreadsheet && (
                <CsvPreviewSection
                  dataset={dataset}
                  previewData={tableImport.previewData!}
                  loadingPreview={tableImport.loadingPreview}
                  selectedPrimaryKey={tableImport.selectedPrimaryKey}
                  setSelectedPrimaryKey={tableImport.setSelectedPrimaryKey}
                  confirmedRelationships={tableImport.confirmedRelationships}
                  setConfirmedRelationships={tableImport.setConfirmedRelationships}
                  selectedListColumns={tableImport.selectedListColumns}
                  setSelectedListColumns={tableImport.setSelectedListColumns}
                />
              )}

              <button
                type="submit"
                disabled={tableImport.uploading || (!tableImport.previewData && !tableImport.spreadsheetPreview) || (tableImport.importMode === 'file' && !tableImport.selectedFile) || (tableImport.importMode === 'url' && !tableImport.fileUrl)}
                style={{
                  padding: '0.75rem 1.5rem',
                  background: tableImport.uploading ? '#ccc' : '#4CAF50',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: tableImport.uploading ? 'not-allowed' : 'pointer'
                }}
              >
                {tableImport.uploading
                  ? 'Adding...'
                  : (tableImport.isSpreadsheet && tableImport.sheetConfigs.filter(s => s.selected).length > 1
                    ? `Add ${tableImport.sheetConfigs.filter(s => s.selected).length} Tables`
                    : 'Add Table')}
              </button>
            </form>
          </div>
        )}

        <div style={{ display: 'grid', gap: '1rem' }}>
          {dataset.tables.map((table) => (
            <TableCard
              key={table.id}
              table={table}
              dataset={dataset}
              isSelected={tableActions.selectedTable === table.id}
              isRenaming={tableActions.renamingTableId === table.id}
              renamingTableName={tableActions.renamingTableName}
              setRenamingTableName={tableActions.setRenamingTableName}
              onStartRename={() => {
                tableActions.setRenamingTableId(table.id)
                tableActions.setRenamingTableName(table.displayName)
              }}
              onCancelRename={() => tableActions.setRenamingTableId(null)}
              onSaveRename={() => tableActions.handleRenameTable(table.id)}
              onViewData={() => tableActions.loadTableData(table.id)}
              onManageKeys={() => keyRelEditor.openKeyEditor(table)}
              onManageColumns={() => columnEditor.loadColumns(table.id)}
              onDelete={() => tableActions.handleDeleteTable(table.id)}
            />
          ))}
        </div>
      </div>

      {tableActions.selectedTable && (
        <TableDataPreview
          tableData={tableActions.tableData}
          loadingData={tableActions.loadingData}
        />
      )}

      {columnEditor.showColumnEditor && (
        <ColumnEditorModal
          columns={columnEditor.columns}
          loadingColumns={columnEditor.loadingColumns}
          onClose={() => columnEditor.setShowColumnEditor(false)}
          onUpdateColumn={columnEditor.updateColumnMetadata}
        />
      )}

      {keyRelEditor.showKeyEditor && (
        <KeyRelationshipEditorModal
          dataset={dataset}
          keyEditorTableId={keyRelEditor.keyEditorTableId}
          keyEditorColumns={keyRelEditor.keyEditorColumns}
          keyEditorLoading={keyRelEditor.keyEditorLoading}
          primaryKeySelection={keyRelEditor.primaryKeySelection}
          setPrimaryKeySelection={keyRelEditor.setPrimaryKeySelection}
          primaryKeySaveDisabled={keyRelEditor.primaryKeySaveDisabled}
          primaryKeySaving={keyRelEditor.primaryKeySaving}
          tableRelationships={keyRelEditor.tableRelationships}
          relationshipForm={keyRelEditor.relationshipForm}
          setRelationshipForm={keyRelEditor.setRelationshipForm}
          referencedColumnsCache={keyRelEditor.referencedColumnsCache}
          referencedColumnsLoading={keyRelEditor.referencedColumnsLoading}
          relationshipSaving={keyRelEditor.relationshipSaving}
          relationshipActionDisabled={keyRelEditor.relationshipActionDisabled}
          onClose={keyRelEditor.closeKeyEditor}
          onSavePrimaryKey={keyRelEditor.handleSavePrimaryKey}
          onAddRelationship={keyRelEditor.handleAddRelationship}
          onDeleteRelationship={keyRelEditor.handleDeleteRelationship}
          onEnsureReferencedColumns={keyRelEditor.ensureReferencedColumns}
        />
      )}
    </div>
  )
}

export default DatasetManage

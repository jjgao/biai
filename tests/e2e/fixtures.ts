import { test as base, expect } from '@playwright/test'
import fs from 'fs'
import path from 'path'

const API_BASE = 'http://localhost:5001/api'
const SERVER_BASE = 'http://localhost:5001'

/**
 * Wait for the API server to be ready (useful in beforeAll when
 * Playwright's webServer only waits for the client on port 3000).
 */
export async function waitForServer(
  timeoutMs = 30_000,
  intervalMs = 500
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${SERVER_BASE}/health`)
      if (res.ok) return
    } catch {
      // server not up yet
    }
    await new Promise((r) => setTimeout(r, intervalMs))
  }
  throw new Error(`API server not ready after ${timeoutMs}ms`)
}

export interface DatasetInfo {
  datasetId: string
  name: string
}

export interface TableInfo {
  tableId: string
  tableName: string
  displayName: string
  rowCount: number
}

export async function apiCreateDataset(
  name: string,
  description = ''
): Promise<DatasetInfo> {
  const res = await fetch(`${API_BASE}/datasets`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, description })
  })
  if (!res.ok) throw new Error(`Create dataset failed: ${res.status}`)
  const data = await res.json()
  return { datasetId: data.dataset.id, name: data.dataset.name }
}

export async function apiUploadCSV(
  datasetId: string,
  filePath: string,
  tableName: string,
  displayName?: string
): Promise<TableInfo> {
  const absolutePath = path.resolve(filePath)
  const fileBuffer = fs.readFileSync(absolutePath)
  const fileName = path.basename(absolutePath)

  const formData = new FormData()
  formData.append('file', new Blob([fileBuffer]), fileName)
  formData.append('tableName', tableName)
  if (displayName) formData.append('displayName', displayName)
  formData.append('delimiter', ',')

  const res = await fetch(`${API_BASE}/datasets/${datasetId}/tables`, {
    method: 'POST',
    body: formData
  })
  if (!res.ok) {
    const errBody = await res.text()
    throw new Error(`Upload CSV failed: ${res.status} - ${errBody}`)
  }
  const data = await res.json()
  return {
    tableId: data.table.id,
    tableName: data.table.name,
    displayName: data.table.displayName,
    rowCount: data.table.rowCount
  }
}

export async function apiDeleteDataset(datasetId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}`, {
    method: 'DELETE'
  })
  if (!res.ok) throw new Error(`Delete dataset failed: ${res.status}`)
}

export async function apiAddRelationship(
  datasetId: string,
  tableId: string,
  foreignKey: string,
  referencedTableId: string,
  referencedColumn: string
): Promise<void> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}/tables/${tableId}/relationships`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      foreignKey,
      referencedTableId,
      referencedColumn,
      type: 'many-to-one'
    })
  })
  if (!res.ok) {
    const errBody = await res.text()
    throw new Error(`Add relationship failed: ${res.status} - ${errBody}`)
  }
}

type TestFixtures = {
  apiHelpers: {
    createDataset: typeof apiCreateDataset
    uploadCSV: typeof apiUploadCSV
    deleteDataset: typeof apiDeleteDataset
    addRelationship: typeof apiAddRelationship
  }
}

export const test = base.extend<TestFixtures>({
  apiHelpers: async ({ }, use) => {
    await use({
      createDataset: apiCreateDataset,
      uploadCSV: apiUploadCSV,
      deleteDataset: apiDeleteDataset,
      addRelationship: apiAddRelationship
    })
  }
})

export { expect }

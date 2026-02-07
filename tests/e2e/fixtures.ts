import { test as base, expect } from '@playwright/test'
import fs from 'fs'
import path from 'path'

const API_BASE = 'http://localhost:5001/api'

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

async function apiCreateDataset(
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

async function apiUploadCSV(
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

async function apiDeleteDataset(datasetId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/datasets/${datasetId}`, {
    method: 'DELETE'
  })
  if (!res.ok) throw new Error(`Delete dataset failed: ${res.status}`)
}

type TestFixtures = {
  apiHelpers: {
    createDataset: typeof apiCreateDataset
    uploadCSV: typeof apiUploadCSV
    deleteDataset: typeof apiDeleteDataset
  }
}

export const test = base.extend<TestFixtures>({
  apiHelpers: async ({}, use) => {
    await use({
      createDataset: apiCreateDataset,
      uploadCSV: apiUploadCSV,
      deleteDataset: apiDeleteDataset
    })
  }
})

export { expect }

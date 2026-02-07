import path from 'path'
import { test, expect, waitForServer, apiImportFromPath, apiDeleteDataset } from './fixtures'

test.describe('Directory Import', () => {
  let datasetId: string | null = null

  test.beforeAll(async () => {
    await waitForServer()
  })

  test.afterAll(async () => {
    if (datasetId) {
      await apiDeleteDataset(datasetId).catch(() => {})
    }
  })

  test('import dataset from local path', async ({ page }) => {
    test.setTimeout(120_000)

    const datasetPath = path.resolve('example_data/gbm_tcga_pan_can_atlas_2018')
    const result = await apiImportFromPath(datasetPath)
    datasetId = result.datasetId

    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    await expect(page.getByRole('button', { name: /Clinical Patients/i })).toBeVisible({ timeout: 20_000 })
    await expect(page.getByRole('button', { name: /Clinical Samples/i })).toBeVisible({ timeout: 20_000 })
  })
})

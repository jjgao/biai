import { test, expect } from './fixtures'

test.describe('Dataset Management', () => {
  let createdDatasetId: string | null = null

  test.afterEach(async ({ apiHelpers }) => {
    if (createdDatasetId) {
      await apiHelpers.deleteDataset(createdDatasetId).catch(() => {})
      createdDatasetId = null
    }
  })

  test('create and list dataset', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Open create form
    await page.getByRole('button', { name: '+ Create Dataset' }).click()
    await expect(page.getByText('Create New Dataset')).toBeVisible()

    // Fill form
    const testName = `E2E Test Dataset ${Date.now()}`
    await page.getByPlaceholder('e.g., TCGA GBM Study').fill(testName)
    await page.getByPlaceholder('Describe this dataset...').fill('Created by e2e test')

    // Submit
    await page.getByRole('button', { name: 'Create Dataset', exact: true }).click()

    // Verify dataset appears in list
    await expect(page.getByText(testName)).toBeVisible({ timeout: 10_000 })

    // Extract dataset ID for cleanup — find the explore link
    const exploreLink = page.getByText('Explore Data').first()
    await expect(exploreLink).toBeVisible()
    const href = await exploreLink.evaluate(
      (el) => el.closest('a')?.getAttribute('href') || ''
    )
    const match = href.match(/\/datasets\/([^/]+)/)
    if (match) createdDatasetId = match[1]
  })

  test('upload CSV to dataset', async ({ page, apiHelpers }) => {
    // Create dataset via API
    const ds = await apiHelpers.createDataset(`E2E Upload Test ${Date.now()}`)
    createdDatasetId = ds.datasetId

    // Navigate to manage page
    await page.goto(`/datasets/${ds.datasetId}/manage`)
    await page.waitForLoadState('networkidle')

    // Click add table
    await page.getByRole('button', { name: '+ Add Table' }).click()

    // Upload file
    const fileInput = page.locator('input[type="file"]')
    await fileInput.setInputFiles('example_data/test-data-geographic.csv')

    // Wait for auto-detection to complete
    await page.waitForTimeout(1000)

    // Set delimiter to comma (the test file is CSV)
    const delimiterSelect = page.locator('select').filter({ has: page.locator('option:has-text("Comma")') })
    if (await delimiterSelect.count() > 0) {
      await delimiterSelect.first().selectOption({ label: 'Comma' })
    }

    // Submit the upload — button text is "Add Table"
    await page.getByRole('button', { name: 'Add Table', exact: true }).click()

    // Verify table appears — wait for the table to show in the tables list
    await expect(page.getByText(/\d+ rows/)).toBeVisible({ timeout: 30_000 })
  })

  test('delete dataset', async ({ page, apiHelpers }) => {
    const ds = await apiHelpers.createDataset(`E2E Delete Test ${Date.now()}`)
    createdDatasetId = ds.datasetId

    await page.goto(`/datasets/${ds.datasetId}/manage`)
    await page.waitForLoadState('networkidle')

    // Handle the confirmation dialog
    page.on('dialog', (dialog) => dialog.accept())

    // Click delete
    await page.getByRole('button', { name: 'Delete Dataset' }).click()

    // Should redirect to /datasets (the Datasets list page, served at /)
    await page.waitForURL(/\/datasets$|^\/$/, { timeout: 10_000 })

    // Wait for page to load
    await page.waitForLoadState('networkidle')

    // Dataset should no longer appear
    await expect(page.getByText(ds.name)).not.toBeVisible()
    createdDatasetId = null // Already deleted
  })
})

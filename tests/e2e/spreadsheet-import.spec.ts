import { test, expect } from './fixtures'

test.describe('Spreadsheet Import', () => {
  let datasetId: string

  test.beforeAll(async () => {
    const ds = await fetch('http://localhost:5001/api/datasets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: `E2E Spreadsheet ${Date.now()}`,
        description: 'Dataset for spreadsheet import e2e tests'
      })
    }).then((r) => r.json())
    datasetId = ds.dataset.id
  })

  test.afterAll(async () => {
    if (datasetId) {
      await fetch(`http://localhost:5001/api/datasets/${datasetId}`, {
        method: 'DELETE'
      }).catch(() => {})
    }
  })

  test('sheet preview', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}/manage`)
    await page.waitForLoadState('networkidle')

    // Open add table form
    await page.getByRole('button', { name: '+ Add Table' }).click()

    // Upload spreadsheet file
    const fileInput = page.locator('input[type="file"]')
    await fileInput.setInputFiles('example_data/clinical_trial_data.xlsx')

    // Wait for sheet preview to appear
    await expect(page.getByText(/Spreadsheet Sheets/)).toBeVisible({ timeout: 15_000 })

    // Verify sheets are listed with row counts
    await expect(page.getByText(/\d+ rows/).first()).toBeVisible()

    // Verify checkboxes are present for sheet selection
    const checkboxes = page.locator('input[type="checkbox"]')
    expect(await checkboxes.count()).toBeGreaterThan(0)
  })

  test('import spreadsheet sheets', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}/manage`)
    await page.waitForLoadState('networkidle')

    // Open add table form
    await page.getByRole('button', { name: '+ Add Table' }).click()

    // Upload spreadsheet file
    const fileInput = page.locator('input[type="file"]')
    await fileInput.setInputFiles('example_data/clinical_trial_data.xlsx')

    // Wait for sheet preview
    await expect(page.getByText(/Spreadsheet Sheets/)).toBeVisible({ timeout: 15_000 })

    // Select a sheet (click first checkbox)
    const checkboxes = page.locator('input[type="checkbox"]')
    await checkboxes.first().check()

    // Submit the import — button text is "Add Table" or "Add N Tables"
    await page.getByRole('button', { name: /Add.*Table/i }).click()

    // Wait for import to complete — tables should appear in the tables list
    // The form closes and we see row counts in the tables section
    await expect(page.getByText(/Tables \(\d+\)/)).toBeVisible({ timeout: 30_000 })
  })
})

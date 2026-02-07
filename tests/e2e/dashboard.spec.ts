import { test, expect, waitForServer } from './fixtures'

test.describe('Dashboard Management', () => {
  let datasetId: string

  test.beforeEach(async ({ apiHelpers }) => {
    await waitForServer()
    // Create fresh dataset for each test to avoid state pollution
    const ds = await apiHelpers.createDataset(
      `E2E Dashboard ${Date.now()}`,
      'Dataset for dashboard tests'
    )
    datasetId = ds.datasetId
    await apiHelpers.uploadCSV(datasetId, 'example_data/test-data-geographic.csv', 'geographic', 'Geographic Data')
  })

  test.afterEach(async ({ apiHelpers }) => {
    if (datasetId) {
      await apiHelpers.deleteDataset(datasetId).catch(() => {})
    }
  })

  test('pin chart, save, clear, load, and delete dashboard', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // 1. Pin a chart — switch to table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Find "Add to dashboard" button for the first chart
    const addToDashboardBtn = page.getByTitle('Add to dashboard').first()
    await addToDashboardBtn.waitFor()
    await addToDashboardBtn.click()

    // Verify button changes to "Remove from dashboard"
    await expect(page.getByTitle('Remove from dashboard').first()).toBeVisible()

    // 2. Verify chart on Dashboard
    await page.getByRole('button', { name: 'Dashboard', exact: true }).click()
    await expect(page.getByText('Your Dashboard is Empty')).not.toBeVisible()
    await expect(page.locator('.js-plotly-plot').first()).toBeVisible()

    // 3. Save Dashboard
    await page.getByRole('button', { name: 'Save Dashboard' }).click()
    const saveDialog = page.getByText('Save Dashboard').first()
    await expect(saveDialog).toBeVisible()

    const nameInput = page.getByPlaceholder('Enter dashboard name...')
    await nameInput.fill('My Test Dashboard')

    await page.getByRole('button', { name: 'Save', exact: true }).click()

    // Dialog should close
    await expect(nameInput).not.toBeVisible()

    // 4. Clear Dashboard
    page.on('dialog', dialog => dialog.accept())
    await page.getByRole('button', { name: 'Clear' }).click()

    // Verify empty state
    await expect(page.getByText('Your Dashboard is Empty')).toBeVisible()

    // 5. Load Dashboard
    await page.getByRole('button', { name: 'Load Dashboard' }).click()

    const dashboardItem = page.getByText('My Test Dashboard').first()
    await expect(dashboardItem).toBeVisible()
    await dashboardItem.click()

    // Verify chart is back
    await expect(page.locator('.js-plotly-plot').first()).toBeVisible()
    await expect(page.getByRole('heading', { name: /Dashboard: My Test Dashboard/ })).toBeVisible()

    // 6. Delete Dashboard
    await page.getByRole('button', { name: 'Manage' }).click()

    await expect(page.getByRole('heading', { name: /Manage Saved Dashboards/ })).toBeVisible()

    const deleteBtn = page.getByRole('button', { name: 'Delete' }).first()
    await deleteBtn.waitFor()
    await deleteBtn.click()

    // Verify it's gone from the list
    await expect(page.getByText('No dashboards saved yet')).toBeVisible()

    // Close Manage dialog
    await page.getByRole('button', { name: 'Close' }).click()

    // 7. Verify Load Dashboard button is disabled
    const loadBtn = page.getByRole('button', { name: 'Load Dashboard' })
    await expect(loadBtn).toBeDisabled()
  })
})

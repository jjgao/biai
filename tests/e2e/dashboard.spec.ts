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
            await apiHelpers.deleteDataset(datasetId).catch(() => { })
        }
    })

    test('pin chart, save, clear, load, and delete dashboard', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}`)
        // Wait for everything to settle
        await page.waitForLoadState('networkidle')

        // 1. Pin a chart
        // Switch to table tab
        await page.getByRole('button', { name: /Geographic Data/ }).click()

        // Find "Add to dashboard" button for the first chart
        // The chart container has a header with actions.
        // We look for the button with title "Add to dashboard"
        const addToDashboardBtn = page.getByTitle('Add to dashboard').first()
        await addToDashboardBtn.waitFor()
        await addToDashboardBtn.click()

        // Verify button changes to "Remove from dashboard"
        await expect(page.getByTitle('Remove from dashboard').first()).toBeVisible()

        // 2. Verify chart on Dashboard
        // Use regex to match "Dashboard" or "Dashboard (1)" but NOT "Load Dashboard" or "Save Dashboard"
        await page.getByRole('button', { name: /^Dashboard/ }).click()
        await expect(page.getByText('Your Dashboard is Empty')).not.toBeVisible()
        // Chart should be visible
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible()

        // 3. Save Dashboard
        await page.getByRole('button', { name: 'Save Dashboard' }).click()
        const saveDialog = page.getByText('Save Dashboard').first() // Title of dialog
        await expect(saveDialog).toBeVisible()

        const nameInput = page.getByPlaceholder('Enter dashboard name...')
        await nameInput.fill('My Test Dashboard')

        // Click "Save" in the dialog (likely the last button or explicitly named)
        // Looking at the code: <button ...>Save</button> inside dialog
        await page.getByRole('button', { name: 'Save', exact: true }).click()

        // Dialog should close
        await expect(nameInput).not.toBeVisible()

        // Header doesn't update immediately as we are just saving the current state
        // await expect(page.getByRole('heading', { name: /Dashboard: My Test Dashboard/ })).toBeVisible()

        // 4. Clear Dashboard
        // Accept confirm dialog
        page.on('dialog', dialog => dialog.accept())
        await page.getByRole('button', { name: 'Clear' }).click()

        // Verify empty state
        await expect(page.getByText('Your Dashboard is Empty')).toBeVisible()

        // 5. Load Dashboard
        await page.getByRole('button', { name: 'Load Dashboard' }).click()

        // Expect "My Test Dashboard" in the list
        const dashboardItem = page.getByText('My Test Dashboard').first()
        await expect(dashboardItem).toBeVisible()
        await dashboardItem.click()

        // Verify chart is back
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible()
        await expect(page.getByRole('heading', { name: /Dashboard: My Test Dashboard/ })).toBeVisible()

        // 6. Delete Dashboard
        await page.getByRole('button', { name: 'Manage' }).click()

        // Wait for manage dialog
        await expect(page.getByRole('heading', { name: /Manage Saved Dashboards/ })).toBeVisible()

        // Since we only have one dashboard, we can just find the Delete button directly
        // or locate it relative to the text more loosely
        const deleteBtn = page.getByRole('button', { name: 'Delete' }).first()
        await deleteBtn.waitFor()
        await deleteBtn.click()

        // Verify it's gone from the list in Manage dialog
        await expect(page.getByText('No dashboards saved yet')).toBeVisible()

        // Close Manage dialog
        await page.getByRole('button', { name: 'Close' }).click()

        // 7. Verify Load Dashboard button is disabled or list is empty
        // If no saved dashboards, Load Dashboard button is disabled.
        const loadBtn = page.getByRole('button', { name: 'Load Dashboard' })
        await expect(loadBtn).toBeDisabled()
    })
})

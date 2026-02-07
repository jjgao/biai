
import { test, expect, waitForServer, apiCreateDataset, apiUploadCSV, apiDeleteDataset } from './fixtures'

test.describe('Settings & View Preferences', () => {
    let datasetId: string

    test.beforeAll(async () => {
        const uniqueId = Date.now().toString()
        const ds = await apiCreateDataset(`settings-test-ds-${uniqueId}`, `Settings Test Dataset ${uniqueId}`)
        expect(ds.datasetId).toBeDefined()
        datasetId = ds.datasetId

        // Upload patients data
        await apiUploadCSV(datasetId, 'example_data/patients.csv', 'patients', 'Patients Data')
    })

    test.afterAll(async () => {
        if (datasetId) {
            await apiDeleteDataset(datasetId)
        }
    })

    test('percentage labels toggle persists', async ({ page }) => {
        test.setTimeout(120_000)


        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // 1. Open Chart Settings
        const settingsButton = page.getByRole('button', { name: 'Chart settings' })
        await expect(settingsButton).toBeVisible()
        await settingsButton.click()

        // 2. Click "Percentages"
        const percentagesButton = page.getByRole('button', { name: 'Percentages' })
        await expect(percentagesButton).toBeVisible()
        await percentagesButton.click()

        // 3. Verify charts show percentage (checking button state)
        // Re-open menu if it closed (it closes on selection)
        if (!await percentagesButton.isVisible()) {
            await settingsButton.click()
        }
        await expect(percentagesButton).toBeVisible()
        const color = await percentagesButton.evaluate((el) => window.getComputedStyle(el).backgroundColor)



        // Check if it's the blue color (approximate check usually safer)
        expect(color).toContain('rgb(25, 118, 210)')

        // 4. Reload page
        console.log('Reloading page...')
        await page.reload()
        await page.waitForLoadState('domcontentloaded') // Use domcontentloaded first
        await page.waitForLoadState('networkidle')
        console.log('Page reloaded')

        // 5. Verify persistence
        await expect(page.getByRole('button', { name: 'Chart settings' })).toBeVisible({ timeout: 30_000 })
        await page.getByRole('button', { name: 'Chart settings' }).click()

        const percentagesButtonAfter = page.getByRole('button', { name: 'Percentages' })
        await expect(percentagesButtonAfter).toBeVisible()
        const colorAfter = await percentagesButtonAfter.evaluate((el) => window.getComputedStyle(el).backgroundColor)



        expect(colorAfter).toContain('rgb(25, 118, 210)')
    })

    test('view preference (chart/table) toggles and persists', async ({ page }) => {
        test.setTimeout(180_000)


        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Click on the table tab (defaults to Dashboard which is empty)
        await page.getByRole('button', { name: /Patients Data/ }).click()



        // Wait for at least one chart
        const firstChart = page.locator('.js-plotly-plot').first()
        await expect(firstChart).toBeVisible({ timeout: 60_000 })

        // Find the card containing the first chart
        // We can assume the chart is inside the card content
        // Let's find the card by traversing up from the chart
        // Or just look for the toggle button that is visible

        // Strategy: Get all cards, find one that has a chart.
        // Actually, simpler: Use locator relative to the chart.
        // The chart is inside a GraphDiv, inside a CardContent, inside a Card.
        // The toggle button is in CardHeader (sibling of CardContent).

        // Let's find the toggle button directly associated with the chart?
        // Not easy.

        // Better: Find any "Switch to table view" button that is visible.
        // Pick the first one.
        const toTableButton = page.locator('button[title="Switch to table view"]').first()
        await expect(toTableButton).toBeVisible()

        // We need to know WHICH card we are toggling to verify later.
        // Let's find the container of this button.
        // The button is in the header. The header has the title.
        // Let's get the title text.
        // The title is in an h4 sibling (or close).
        // Structure: div > [h4, div(buttons)]

        // Let's click it.
        await toTableButton.click()

        // Now verify THAT card has a table.
        // Since we clicked "first" button, the "first" card should now have a table.
        // And NO chart.

        // But wait, if we have multiple charts, "first" button corresponds to the first chart card.
        // After clicking, that card becomes a table.
        // So the "first" chart locally might be the SECOND chart now (if order shifts or removed).
        // Actually table view replaces chart view.

        // So searching for '.js-plotly-plot' again will return the NEXT chart.
        // Searching for 'table' will return the table.

        const table = page.locator('table').first()
        await expect(table).toBeVisible()

        // And there should be a "Switch to chart view" button.
        const toChartButton = page.locator('button[title="Switch to chart view"]').first()
        await expect(toChartButton).toBeVisible()

        // 3. Reload
        console.log('Reloading page...')
        await page.reload()
        await page.waitForLoadState('domcontentloaded')
        await page.waitForLoadState('networkidle')
        console.log('Page reloaded')

        // Click on the table tab again as reload resets to Dashboard
        await page.getByRole('button', { name: /Patients Data/ }).click()

        // 4. Verify persistence
        // The first card should still be a table.
        // So 'table' first should be visible.
        await expect(page.locator('table').first()).toBeVisible({ timeout: 60_000 })

        // And "Switch to chart view" button should be visible.
        const toChartButtonAfter = page.locator('button[title="Switch to chart view"]').first()
        await expect(toChartButtonAfter).toBeVisible()

        // 5. Restore
        await toChartButtonAfter.click()

        // 6. Verify it's a chart again
        // Now we should have NO tables (assuming only one was toggled)
        // Or at least checking that a chart appears where the table was?
        // Just start of 'toChartButton' becoming 'toTableButton'
        await expect(page.locator('button[title="Switch to table view"]').first()).toBeVisible()
    })
})


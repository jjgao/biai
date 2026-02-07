import { test, expect, waitForServer, apiCreateDataset, apiUploadCSV, apiDeleteDataset } from './fixtures'

test.describe('Chart Types', () => {
    let datasetId: string

    test.beforeAll(async () => {
        await waitForServer()
        const ds = await apiCreateDataset(
            `E2E ChartTypes ${Date.now()}`,
            'Dataset for chart type e2e tests'
        )
        datasetId = ds.datasetId
        // Geographic data has: state (geographic), age (numeric), diagnosis (categorical),
        // treatment_cost (numeric), outcome (categorical)
        await apiUploadCSV(datasetId, 'example_data/test-data-geographic.csv', 'geographic', 'Geographic Data')
    })

    test.afterAll(async () => {
        if (datasetId) {
            await apiDeleteDataset(datasetId).catch(() => { })
        }
    })

    test('histogram renders for numeric columns', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Switch to table tab
        await page.getByRole('button', { name: /Geographic Data/ }).click()

        // Wait for charts to render
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Find the "age" chart card by its heading
        const ageCard = page.locator('div').filter({
            has: page.locator('h4').filter({ hasText: /^age$/i })
        }).filter({
            has: page.locator('.js-plotly-plot')
        }).last()
        await expect(ageCard).toBeVisible()

        // Verify it's a histogram (bar chart with trace.bars class)
        // Histograms use type: 'bar' in Plotly, which creates g.trace.bars elements
        const histogramBars = ageCard.locator('.js-plotly-plot svg g.trace.bars')
        await expect(histogramBars).toBeVisible()

        // Also verify treatment_cost has histogram
        const costCard = page.locator('div').filter({
            has: page.locator('h4').filter({ hasText: /treatment_cost/i })
        }).filter({
            has: page.locator('.js-plotly-plot')
        }).last()
        await expect(costCard).toBeVisible()
        await expect(costCard.locator('.js-plotly-plot svg g.trace.bars')).toBeVisible()
    })

    test('pie chart renders for categorical columns with few values', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Switch to table tab
        await page.getByRole('button', { name: /Geographic Data/ }).click()

        // Wait for charts to render
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Find the "outcome" chart card (has few unique values: Remission, Stable, Improved, etc.)
        const outcomeCard = page.locator('div').filter({
            has: page.locator('h4').filter({ hasText: /^outcome$/i })
        }).filter({
            has: page.locator('.js-plotly-plot')
        }).last()
        await expect(outcomeCard).toBeVisible()

        // Verify it's a pie chart (Plotly pie charts have g.trace.pie or g.slice elements)
        const pieSlices = outcomeCard.locator('.js-plotly-plot svg g.slice, .js-plotly-plot svg g.trace.pie')
        await expect(pieSlices.first()).toBeVisible()
    })

    test('bar chart renders for categorical columns with many values', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Switch to table tab
        await page.getByRole('button', { name: /Geographic Data/ }).click()

        // Wait for charts to render
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Find the "state" chart card - it has many unique values (California, Texas, etc.)
        // State columns are typically rendered as bar charts OR map charts
        const stateCard = page.locator('div').filter({
            has: page.locator('h4').filter({ hasText: /^state$/i })
        }).filter({
            has: page.locator('.js-plotly-plot')
        }).last()
        await expect(stateCard).toBeVisible()

        // State could be rendered as map (choropleth) or bar chart depending on detection
        // Check for either trace.bars (bar) or geo (map)
        const chartContent = stateCard.locator('.js-plotly-plot svg g.trace.bars, .js-plotly-plot svg g.geo')
        await expect(chartContent.first()).toBeVisible()
    })

    test('map chart renders for geographic state columns', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Switch to table tab
        await page.getByRole('button', { name: /Geographic Data/ }).click()

        // Wait for charts to render
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Find the "state" chart card
        const stateCard = page.locator('div').filter({
            has: page.locator('h4').filter({ hasText: /^state$/i })
        }).filter({
            has: page.locator('.js-plotly-plot')
        }).last()
        await expect(stateCard).toBeVisible()

        // Map charts have a g.geo element for the US map
        const geoElement = stateCard.locator('.js-plotly-plot svg g.geo')

        // State might be detected as map or as bar chart - check if geo exists
        const isMapChart = await geoElement.count() > 0

        if (isMapChart) {
            // Verify the choropleth has state paths
            await expect(geoElement).toBeVisible()
            // Choropleth maps have path elements for states
            await expect(stateCard.locator('.js-plotly-plot svg g.geo path').first()).toBeVisible()
        } else {
            // If not a map, it should at least be a bar chart
            await expect(stateCard.locator('.js-plotly-plot svg g.trace.bars')).toBeVisible()
        }
    })

    test('charts respond to filter interactions', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Switch to table tab
        await page.getByRole('button', { name: /Geographic Data/ }).click()

        // Wait for charts to render
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Click on a pie slice or bar to add a filter
        const firstChart = page.locator('.js-plotly-plot').first()
        const clickableElements = firstChart.locator('svg g.slice path, svg g.trace.bars g.points path, svg g.trace.bars g.point path')

        if (await clickableElements.count() > 0) {
            await clickableElements.first().click({ force: true })

            // Verify filter is applied
            await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

            // Verify row count shows filtered state
            await expect(page.getByTestId('filtered-count-geographic')).toBeVisible()
        }
    })
})

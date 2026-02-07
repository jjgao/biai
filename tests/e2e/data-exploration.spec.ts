import { test, expect, waitForServer, apiCreateDataset, apiUploadCSV, apiDeleteDataset } from './fixtures'

test.describe('Data Exploration', () => {
  let datasetId: string

  test.beforeAll(async () => {
    await waitForServer()
    const ds = await apiCreateDataset(
      `E2E Exploration ${Date.now()}`,
      'Dataset for exploration e2e tests'
    )
    datasetId = ds.datasetId
    await apiUploadCSV(datasetId, 'example_data/test-data-geographic.csv', 'geographic', 'Geographic Data')
  })

  test.afterAll(async () => {
    if (datasetId) {
      await apiDeleteDataset(datasetId).catch(() => {})
    }
  })

  test('view charts', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Explorer defaults to Dashboard tab — click the table tab to see charts
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for Plotly charts to render
    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Should have multiple charts for the dataset columns
    const chartCount = await charts.count()
    expect(chartCount).toBeGreaterThan(0)
  })

  test('filter by clicking chart', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Click the table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for charts to render
    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Click on a bar in the first chart to apply a filter
    const firstChart = charts.first()
    const svgBars = firstChart.locator('g.trace.bars g.points path, g.trace.bars g.point path')
    if ((await svgBars.count()) > 0) {
      await svgBars.first().click({ force: true })
    } else {
      await firstChart.click({ position: { x: 100, y: 100 } })
    }

    // Verify filter applied
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })
  })

  test('clear filters', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Click the table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for charts
    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Click a chart bar to create a filter
    const firstChart = charts.first()
    const svgBars = firstChart.locator('g.trace.bars g.points path, g.trace.bars g.point path')
    if ((await svgBars.count()) > 0) {
      await svgBars.first().click({ force: true })
    } else {
      await firstChart.click({ position: { x: 100, y: 100 } })
    }

    // Wait for filter to appear
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

    // Click Clear All
    await page.getByRole('button', { name: 'Clear All' }).click()

    // Verify filters cleared
    await expect(page.getByText('Active Filters:')).not.toBeVisible({ timeout: 5_000 })
  })

  test('tab switching', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Dashboard tab should be visible and active by default
    const dashboardTab = page.getByRole('button', { name: 'Dashboard', exact: true })
    await expect(dashboardTab).toBeVisible({ timeout: 5_000 })

    // Table tab should be visible
    const tableTab = page.getByRole('button', { name: /Geographic Data/ })
    await expect(tableTab).toBeVisible()

    // Default view shows empty dashboard
    await expect(page.getByText('Your Dashboard is Empty')).toBeVisible()

    // Click table tab — charts should appear
    await tableTab.click()
    await expect(page.getByText('Your Dashboard is Empty')).not.toBeVisible()
    await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

    // Click back to Dashboard tab
    await dashboardTab.click()
    await expect(page.getByText('Your Dashboard is Empty')).toBeVisible({ timeout: 5_000 })
  })
})

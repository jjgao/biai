import { test, expect, waitForServer, apiCreateDataset, apiUploadCSV, apiDeleteDataset } from './fixtures'

test.describe('Filter Presets', () => {
  let datasetId: string

  test.beforeAll(async () => {
    await waitForServer()
    const ds = await apiCreateDataset(
      `E2E Presets ${Date.now()}`,
      'Dataset for filter preset e2e tests'
    )
    datasetId = ds.datasetId
    await apiUploadCSV(datasetId, 'example_data/test-data-geographic.csv', 'geographic', 'Geographic Data')
  })

  test.afterAll(async () => {
    if (datasetId) {
      await apiDeleteDataset(datasetId).catch(() => {})
    }
  })

  test('save filter preset', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Click the table tab (explorer defaults to Dashboard)
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for charts to render
    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Click a chart bar to apply a filter
    const firstChart = charts.first()
    const svgBars = firstChart.locator('g.trace.bars g.points path, g.trace.bars g.point path')
    if ((await svgBars.count()) > 0) {
      await svgBars.first().click({ force: true })
    } else {
      await firstChart.click({ position: { x: 100, y: 100 } })
    }

    // Wait for filter
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

    // Click Save Filter
    await page.getByRole('button', { name: 'Save Filter' }).click()

    // Enter preset name
    const presetName = `Test Preset ${Date.now()}`
    await page.getByPlaceholder('Enter filter name...').fill(presetName)

    // Click Save in the dialog
    await page.getByRole('button', { name: 'Save', exact: true }).click()

    // Verify preset bar shows the saved filter count
    await expect(page.getByText(/saved filter/)).toBeVisible({ timeout: 5_000 })
  })

  test('load filter preset', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Click the table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Apply a filter
    const firstChart = charts.first()
    const svgBars = firstChart.locator('g.trace.bars g.points path, g.trace.bars g.point path')
    if ((await svgBars.count()) > 0) {
      await svgBars.first().click({ force: true })
    } else {
      await firstChart.click({ position: { x: 100, y: 100 } })
    }
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

    // Save the preset
    await page.getByRole('button', { name: 'Save Filter' }).click()
    const presetName = `Load Test ${Date.now()}`
    await page.getByPlaceholder('Enter filter name...').fill(presetName)
    await page.getByRole('button', { name: 'Save', exact: true }).click()
    await expect(page.getByText(/saved filter/)).toBeVisible({ timeout: 5_000 })

    // Clear filters
    await page.getByRole('button', { name: 'Clear All' }).click()
    await expect(page.getByText('Active Filters:')).not.toBeVisible({ timeout: 5_000 })

    // Load the preset
    await page.getByRole('button', { name: 'Load Filter' }).click()
    await page.getByText(presetName).click()

    // Verify filters restored
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })
  })

  test('URL sharing', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Click the table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Apply a filter
    const firstChart = charts.first()
    const svgBars = firstChart.locator('g.trace.bars g.points path, g.trace.bars g.point path')
    if ((await svgBars.count()) > 0) {
      await svgBars.first().click({ force: true })
    } else {
      await firstChart.click({ position: { x: 100, y: 100 } })
    }
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

    // Get the current URL with hash
    const urlWithFilters = page.url()
    expect(urlWithFilters).toContain('#')

    // Navigate away, then back to the URL with filters
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    await page.goto(urlWithFilters)
    await page.waitForLoadState('networkidle')

    // Click the table tab again
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for charts to load
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Verify filters are restored from URL
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 10_000 })
  })
})

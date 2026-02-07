import { test, expect, waitForServer, apiCreateDataset, apiUploadCSV, apiDeleteDataset } from './fixtures'

test.describe('Advanced Filters', () => {
  let datasetId: string

  test.beforeAll(async () => {
    await waitForServer()
    const ds = await apiCreateDataset(
      `E2E Filters ${Date.now()}`,
      'Dataset for advanced filter e2e tests'
    )
    datasetId = ds.datasetId
    await apiUploadCSV(datasetId, 'example_data/test-data-geographic.csv', 'geographic', 'Geographic Data')
  })

  test.afterAll(async () => {
    if (datasetId) {
      await apiDeleteDataset(datasetId).catch(() => {})
    }
  })

  test('numeric range filter with custom min/max', async ({ page }) => {
    test.setTimeout(60_000)
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Switch to table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for charts to render
    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Find the "age" chart card by its heading, then open filter menu
    const ageCard = page.locator('div').filter({
      has: page.locator('h4').filter({ hasText: /^age$/ })
    }).filter({
      has: page.locator('.js-plotly-plot')
    }).last()
    await expect(ageCard).toBeVisible()

    // Click filter button (⚲)
    await ageCard.getByTitle('Filter values').click()

    // Numeric filter menu should appear with From/To inputs
    const fromInput = page.getByLabel('From')
    const toInput = page.getByLabel('To')
    await expect(fromInput).toBeVisible()
    await expect(toInput).toBeVisible()

    // Enter custom range
    await fromInput.fill('40')
    await toInput.fill('60')

    // Click Apply
    await page.getByRole('button', { name: 'Apply' }).click()

    // Verify filter is active
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

    // Verify row count shows filtered state
    await expect(page.getByTestId('filtered-count-geographic')).toBeVisible()
  })

  test('NOT filter toggle on categorical filter', async ({ page }) => {
    test.setTimeout(60_000)
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Switch to table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for charts
    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Click a bar on a categorical chart to apply filter
    const firstChart = charts.first()
    const svgBars = firstChart.locator('g.trace.bars g.points path, g.trace.bars g.point path')
    if ((await svgBars.count()) > 0) {
      await svgBars.first().click({ force: true })
    } else {
      await firstChart.click({ position: { x: 100, y: 100 } })
    }

    // Verify filter appears
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

    // Click the NOT toggle (¬) on the filter chip
    await page.getByTitle('Add NOT').first().click()

    // Verify NOT badge appears on the chip
    await expect(page.locator('span').filter({ hasText: /^NOT$/ })).toBeVisible()

    // Click NOT toggle again to remove it
    await page.getByTitle('Remove NOT').first().click()

    // Verify NOT badge is gone
    await expect(page.locator('span').filter({ hasText: /^NOT$/ })).not.toBeVisible()
  })

  test('remove individual filter via chip close button', async ({ page }) => {
    test.setTimeout(60_000)
    await page.goto(`/datasets/${datasetId}`)
    await page.waitForLoadState('networkidle')

    // Switch to table tab
    await page.getByRole('button', { name: /Geographic Data/ }).click()

    // Wait for charts
    const charts = page.locator('.js-plotly-plot')
    await expect(charts.first()).toBeVisible({ timeout: 15_000 })

    // Apply a categorical filter by clicking a chart bar
    const firstChart = charts.first()
    const svgBars = firstChart.locator('g.trace.bars g.points path, g.trace.bars g.point path')
    if ((await svgBars.count()) > 0) {
      await svgBars.first().click({ force: true })
    } else {
      await firstChart.click({ position: { x: 100, y: 100 } })
    }

    // Verify filter appears
    await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 5_000 })

    // Find the × button on the filter chip and click it
    // The chip has a button with × text for removing the filter
    const removeBtn = page.locator('button').filter({ hasText: '×' }).first()
    await removeBtn.click()

    // Verify filters cleared
    await expect(page.getByText('Active Filters:')).not.toBeVisible({ timeout: 5_000 })
  })
})

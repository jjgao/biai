import { test, expect, waitForServer, apiCreateDataset, apiDeleteDataset } from './fixtures'

test.describe('Navigation & Routing', () => {
  test.beforeAll(async () => {
    await waitForServer()
  })

  test('home page shows dataset list', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Should show the datasets page with create button
    await expect(page.getByRole('button', { name: '+ Create Dataset' })).toBeVisible()

    // Navbar should be visible
    await expect(page.getByText('BIAI').first()).toBeVisible()
    await expect(page.getByText('Datasets').first()).toBeVisible()
  })

  test('invalid dataset ID shows error', async ({ page }) => {
    await page.goto('/datasets/nonexistent-id-12345')
    await page.waitForLoadState('networkidle')

    // Explorer shows an error for non-existent dataset
    await expect(page.getByText(/Error.*404|Dataset not found/)).toBeVisible({ timeout: 10_000 })
  })

  test('invalid manage page shows not found', async ({ page }) => {
    await page.goto('/datasets/nonexistent-id-12345/manage')
    await page.waitForLoadState('networkidle')

    await expect(page.getByText('Dataset not found')).toBeVisible({ timeout: 10_000 })
  })

  test('navigate from dataset list to explorer and back', async ({ page }) => {
    const ds = await apiCreateDataset(`E2E Nav ${Date.now()}`)

    try {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Find the dataset and click Explore Data
      const exploreBtn = page.getByText('Explore Data').first()
      await expect(exploreBtn).toBeVisible({ timeout: 10_000 })
      await exploreBtn.click()

      // Should navigate to explorer page
      await page.waitForURL(/\/datasets\//)
      await expect(page.getByRole('button', { name: 'Dashboard', exact: true })).toBeVisible({ timeout: 15_000 })

      // Navigate back via navbar "Datasets" link
      await page.getByRole('link', { name: 'Datasets' }).click()
      await page.waitForURL(/\/$|\/datasets$/)
      await expect(page.getByRole('button', { name: '+ Create Dataset' })).toBeVisible()
    } finally {
      await apiDeleteDataset(ds.datasetId).catch(() => {})
    }
  })
})

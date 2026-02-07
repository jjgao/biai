import { test, expect, waitForServer, apiListDatabases, apiConnectDatabase, apiDeleteDataset } from './fixtures'

test.describe('Database Connection', () => {
  let datasetId: string | null = null

  test.beforeAll(async () => {
    await waitForServer()
  })

  test.afterAll(async () => {
    if (datasetId) {
      await apiDeleteDataset(datasetId).catch(() => {})
    }
  })

  test('list databases and connect to ClickHouse', async ({ page }) => {
    test.setTimeout(90_000)

    const databases = await apiListDatabases({ host: 'localhost', port: 8123 })
    expect(databases).toContain('biai')

    const displayName = `Connected DB ${Date.now()}`
    const connected = await apiConnectDatabase({
      databaseName: 'biai',
      displayName,
      host: 'localhost',
      port: 8123
    })
    datasetId = connected.datasetId

    await page.goto('/')
    await page.waitForLoadState('networkidle')

    await expect(page.getByRole('heading', { name: displayName })).toBeVisible({ timeout: 15_000 })
    await expect(page.getByText('Connected', { exact: true })).toBeVisible()
    await expect(page.getByText('Host:')).toBeVisible()
  })
})

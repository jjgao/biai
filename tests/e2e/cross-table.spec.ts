import { test, expect, waitForServer, apiCreateDataset, apiUploadCSV, apiDeleteDataset, apiAddRelationship } from './fixtures'

test.describe('Cross-Table Filtering', () => {
    let datasetId: string

    test.beforeAll(async () => {
        await waitForServer()
        const ds = await apiCreateDataset(
            `E2E CrossTable ${Date.now()}`,
            'Dataset for cross-table filtering tests'
        )
        datasetId = ds.datasetId

        const patients = await apiUploadCSV(datasetId, 'example_data/patients.csv', 'patients', 'Patients')
        const treatments = await apiUploadCSV(datasetId, 'example_data/treatments.csv', 'treatments', 'Treatments')

        // Add relationship: treatments.patient_id -> patients.patient_id
        await apiAddRelationship(
            datasetId,
            treatments.tableId,
            'patient_id',
            patients.tableId,
            'patient_id'
        )
    })

    test.afterAll(async () => {
        if (datasetId) {
            await apiDeleteDataset(datasetId).catch(() => { })
        }
    })

    test('filtering parent table filters child table', async ({ page }) => {
        test.setTimeout(90_000);
        await page.goto(`/datasets/${datasetId}`)
        // Network idle can be flaky if there's polling, just wait for DOM and then specific elements
        await page.waitForLoadState('domcontentloaded')

        // Wait for Dashboard tab to verify app loaded
        // Using regex to match "Dashboard" or "Dashboard (1)" but NOT "Load Dashboard"
        await expect(page.getByRole('button', { name: /^Dashboard/ })).toBeVisible({ timeout: 30_000 })

        // 1. Go to Patients tab
        // Button includes chart count e.g. "Patients (3)"
        await page.getByRole('button', { name: /Patients/i }).click()

        // 2. Filter by Gender
        // Find the Gender chart card
        // The chart title 'gender' is displayed in the card header (which also has buttons, so substring match needed)
        const genderCard = page.locator('div').filter({ has: page.locator('.js-plotly-plot') }).filter({ hasText: /gender/i }).last()
        await expect(genderCard).toBeVisible()

        // Click the first slice/bar
        // Pie charts use g.slice path, Bar charts use g.point path
        const chartElement = genderCard.locator('g.slice path, g.point path').first()
        await chartElement.click({ force: true })

        // Verify filter is active
        await expect(page.getByText('Active Filters:')).toBeVisible()

        // 3. Verify Patients count shows filtering
        // Use data-testid for robust selection
        await expect(page.getByTestId('filtered-count-patients')).toBeVisible()
        await expect(page.getByTestId('total-count-patients')).toContainText('5')

        // 4. Go to Treatments tab
        await page.getByRole('button', { name: /Treatments/i }).click()

        // 5. Verify Treatments count is filtered
        // Use data-testid for robust selection
        await expect(page.getByTestId('filtered-count-treatments')).toBeVisible()
        await expect(page.getByTestId('total-count-treatments')).toContainText('6')

        // Verify "linked" filter badge
        // The badge often has text like "+1 linked" or similar title
        const linkedBadge = page.locator('div[title*="propagated from related tables"]')
        await expect(linkedBadge).toBeVisible()
    })

    test('filtering child table filters parent table', async ({ page }) => {
        test.setTimeout(90_000);
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('domcontentloaded')

        // 1. Go to Treatments tab
        await page.getByRole('button', { name: /Treatments/i }).click()

        // 2. Filter by column (e.g. Drug or Outcome)
        // Filter by 'drug' (probably pie chart)
        const drugCard = page.locator('div').filter({ has: page.locator('.js-plotly-plot') }).filter({ hasText: /drug/i }).last()
        await expect(drugCard).toBeVisible()

        await drugCard.locator('g.slice path, g.point path').first().click({ force: true })

        // Verify filter active
        await expect(page.getByText('Active Filters:')).toBeVisible()

        // 3. Go to Patients tab
        await page.getByRole('button', { name: /Patients/i }).click()

        // 4. Verify propagated filter
        const linkedBadge = page.locator('div[title*="propagated from related tables"]')
        await expect(linkedBadge).toBeVisible()

        // Check count is filtered (subset of 5)
        await expect(page.getByText('/ 5').first()).toBeVisible()
    })
})

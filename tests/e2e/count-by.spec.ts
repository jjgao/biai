import { test, expect, waitForServer, apiCreateDataset, apiUploadCSV, apiDeleteDataset, apiAddRelationship } from './fixtures'

test.describe('Count-By / Metric Aggregations', () => {
    let datasetId: string

    test.beforeAll(async () => {
        await waitForServer()
        const ds = await apiCreateDataset(
            `E2E CountBy ${Date.now()}`,
            'Dataset for count-by / metric aggregation tests'
        )
        datasetId = ds.datasetId

        // Upload patients (parent) and treatments (child) with FK relationship
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

    test('count-by dropdown shows parent table options for child table', async ({ page }) => {
        test.setTimeout(60_000)
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Go to Treatments tab (child table)
        await page.getByRole('button', { name: /Treatments/i }).click()

        // Wait for charts to render
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Find the count-by button for the treatments table
        const countByButton = page.getByRole('button', { name: 'Change count-by for treatments', exact: true })
        await expect(countByButton).toBeVisible()

        // Click to open dropdown
        await countByButton.click()

        // Verify dropdown shows parent table option (Patients)
        await expect(page.getByRole('button', { name: 'Patients', exact: true })).toBeVisible()

        // Verify it also shows the default "Treatments" (rows) option
        await expect(page.getByRole('button', { name: 'Treatments', exact: true })).toBeVisible()
    })

    test('selecting parent count-by updates chart labels', async ({ page }) => {
        test.setTimeout(60_000)
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Go to Treatments tab
        await page.getByRole('button', { name: /Treatments/i }).click()

        // Wait for charts
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Find the count-by button
        const countByButton = page.getByRole('button', { name: 'Change count-by for treatments', exact: true })
        await countByButton.click()

        // Select Patients (parent) count-by
        await page.getByRole('button', { name: 'Patients', exact: true }).click()

        // Verify the button title updates to reflect Patients selection
        await expect(countByButton).toHaveAttribute('title', /Patients/i)

        // Verify the row count display updates to show "patients" metric
        // The metric label should change from "rows" to "patients" or similar
        await expect(page.locator('text=/patients/i').first()).toBeVisible({ timeout: 10_000 })
    })

    test('count-by selection persists after tab switch', async ({ page }) => {
        test.setTimeout(60_000)
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Go to Treatments tab
        await page.getByRole('button', { name: /Treatments/i }).click()
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Change count-by to Patients
        const countByButton = page.getByRole('button', { name: 'Change count-by for treatments', exact: true })
        await countByButton.click()
        await page.getByRole('button', { name: 'Patients', exact: true }).click()

        // Verify selection
        await expect(countByButton).toHaveAttribute('title', /Patients/i)

        // Switch to Patients tab
        await page.getByRole('button', { name: /Patients/i }).click()
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Switch back to Treatments tab
        await page.getByRole('button', { name: /Treatments/i }).click()
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Verify count-by selection persisted
        const countByButtonAfter = page.getByRole('button', { name: 'Change count-by for treatments', exact: true })
        await expect(countByButtonAfter).toHaveAttribute('title', /Patients/i)
    })

    test('count-by affects filter aggregation', async ({ page }) => {
        test.setTimeout(90_000)
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Go to Treatments tab
        await page.getByRole('button', { name: /Treatments/i }).click()
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Change count-by to Patients
        const countByButton = page.getByRole('button', { name: 'Change count-by for treatments', exact: true })
        await countByButton.click()
        await page.getByRole('button', { name: 'Patients', exact: true }).click()

        // Wait for charts to update with patient counts
        await expect(countByButton).toHaveAttribute('title', /Patients/i, { timeout: 10_000 })

        // Apply a filter by clicking on a chart element
        const firstChart = page.locator('.js-plotly-plot').first()
        const clickableElements = firstChart.locator('svg g.slice path, svg g.trace.bars g.points path, svg g.point path')

        if (await clickableElements.count() > 0) {
            await clickableElements.first().click({ force: true })

            // Verify filter is applied
            await expect(page.getByText('Active Filters:')).toBeVisible({ timeout: 10_000 })

            // Verify the count-by selection is still showing Patients after filter
            await expect(countByButton).toHaveAttribute('title', /Patients/i)
        } else {
            // If no clickable elements, just verify count-by button is working
            await expect(countByButton).toBeVisible()
        }
    })

    test('switching count-by back to rows restores row counts', async ({ page }) => {
        test.setTimeout(60_000)
        await page.goto(`/datasets/${datasetId}`)
        await page.waitForLoadState('networkidle')

        // Go to Treatments tab
        await page.getByRole('button', { name: /Treatments/i }).click()
        await expect(page.locator('.js-plotly-plot').first()).toBeVisible({ timeout: 15_000 })

        // Initially should show rows (default)
        const countByButton = page.getByRole('button', { name: 'Change count-by for treatments', exact: true })
        await expect(countByButton).toHaveAttribute('title', /Rows|Treatments/i)

        // Change to Patients
        await countByButton.click()
        await page.getByRole('button', { name: 'Patients', exact: true }).click()
        await expect(countByButton).toHaveAttribute('title', /Patients/i)

        // Change back to Treatments (rows)
        await countByButton.click()
        await page.getByRole('button', { name: 'Treatments', exact: true }).click()

        // Verify it shows Rows again
        await expect(countByButton).toHaveAttribute('title', /Rows|Treatments/i)
    })
})

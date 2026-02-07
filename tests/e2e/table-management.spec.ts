import { test, expect, waitForServer } from './fixtures'

test.describe('Table Management', () => {
    test.setTimeout(120_000)
    let datasetId: string

    test.beforeEach(async ({ apiHelpers }) => {
        await waitForServer()
        const ds = await apiHelpers.createDataset(
            `E2E TableManage ${Date.now()}`,
            'Dataset for table management tests'
        )
        datasetId = ds.datasetId
        await apiHelpers.uploadCSV(datasetId, 'example_data/test-data-geographic.csv', 'geographic', 'Geographic Data')
        await apiHelpers.uploadCSV(datasetId, 'example_data/patients.csv', 'patients', 'Patients')
        await apiHelpers.uploadCSV(datasetId, 'example_data/treatments.csv', 'treatments', 'Treatments')
    })

    test.afterEach(async ({ apiHelpers }) => {
        if (datasetId) {
            await apiHelpers.deleteDataset(datasetId).catch(() => { })
        }
    })

    test('rename table', async ({ page }) => {
        // Navigate directly to Manage page to avoid ambiguous button clicks
        await page.goto(`/datasets/${datasetId}/manage`)
        await page.waitForLoadState('domcontentloaded')

        // Wait for "Delete Dataset" button to ensure page loaded
        await expect(page.getByRole('button', { name: 'Delete Dataset' })).toBeVisible()

        // Ensure table list is loaded
        await expect(page.getByText('Geographic Data')).toBeVisible()

        // Locate the table card directly
        const tableCard = page.getByTestId('table-card-geographic')
        await expect(tableCard).toBeVisible()

        // Find rename button (pencil icon)
        const renameBtn = tableCard.getByTestId('rename-table-btn')
        await expect(renameBtn).toBeVisible()
        await renameBtn.click()

        // Input should appear
        const nameInput = tableCard.getByTestId('rename-table-input')
        await expect(nameInput).toBeVisible()
        await expect(nameInput).toHaveValue('Geographic Data')

        // Change name
        await nameInput.fill('Geo Data Renamed')
        await page.waitForTimeout(100)

        // Wait for PATCH request
        const renamePromise = page.waitForResponse(resp =>
            resp.url().includes('/tables/') && resp.request().method() === 'PATCH' && resp.status() === 200
        )

        await tableCard.getByTestId('save-rename-btn').click()
        await renamePromise

        // Verify new name locally
        await expect(page.getByText('Geo Data Renamed')).toBeVisible()

        // Verify persistence by reloading
        await page.reload()
        await expect(page.getByText('Geo Data Renamed')).toBeVisible()
    })

    test('manage columns metadata', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}/manage`)
        await page.waitForLoadState('domcontentloaded')
        await expect(page.getByText('Geographic Data')).toBeVisible()

        // Locate the table card directly
        const tableCard = page.getByTestId('table-card-geographic')

        // Click Manage Columns within that card
        await tableCard.getByRole('button', { name: 'Manage Columns' }).click()

        // Modal should appear
        await expect(page.getByRole('heading', { name: 'Manage Column Metadata' })).toBeVisible()

        // Find card for column "state" using data-testid
        const colCard = page.getByTestId('column-card-state')
        await expect(colCard).toBeVisible()

        // Edit Display Name
        const displayNameInput = colCard.locator('input[type="text"]').first()

        // Prepare to intercept update
        const updateNamePromise = page.waitForResponse(resp =>
            resp.url().includes('/columns/state') &&
            resp.request().method() === 'PATCH' &&
            resp.status() === 200
        )

        await displayNameInput.fill('State Name')
        await displayNameInput.blur()
        await updateNamePromise // Wait for save

        // Edit Description
        const descInput = colCard.locator('textarea').first()
        const updateDescPromise = page.waitForResponse(resp =>
            resp.url().includes('/columns/state') &&
            resp.request().method() === 'PATCH' &&
            resp.status() === 200
        )
        await descInput.fill('The US State name')
        await descInput.blur()
        await updateDescPromise

        // Hide Column
        const hideCheckbox = colCard.getByRole('checkbox', { name: 'Hide this column' })
        const updateHidePromise = page.waitForResponse(resp =>
            resp.url().includes('/columns/state') &&
            resp.request().method() === 'PATCH' &&
            resp.status() === 200
        )
        await hideCheckbox.check()
        await updateHidePromise

        // Close modal
        await page.getByRole('button', { name: '×' }).click()

        // Reload and check persistence
        await page.reload()
        await expect(page.getByText('Geographic Data')).toBeVisible()

        // Locate card again
        const tableCardAfterReload = page.getByTestId('table-card-geographic')
        await expect(tableCardAfterReload).toBeVisible()
        await tableCardAfterReload.getByRole('button', { name: 'Manage Columns' }).click()

        await expect(page.getByRole('heading', { name: 'Manage Column Metadata' })).toBeVisible()

        // Locate card again using data-testid
        const updatedColCard = page.getByTestId('column-card-state')

        await expect(updatedColCard.locator('input[type="text"]').first()).toHaveValue('State Name')
        await expect(updatedColCard.locator('textarea').first()).toHaveValue('The US State name')
        await expect(updatedColCard.getByRole('checkbox', { name: 'Hide this column' })).toBeChecked()
    })

    test('manage keys and relationships', async ({ page }) => {
        await page.goto(`/datasets/${datasetId}/manage`)
        await page.waitForLoadState('domcontentloaded')

        // Find Patients card and open Manage Keys
        const patientsCard = page.getByTestId('table-card-patients')
        await patientsCard.getByRole('button', { name: 'Manage Keys' }).click()

        // Key Editor Modal should appear
        const modal = page.locator('h3', { hasText: 'Manage Keys & Relationships' }).locator('xpath=../..')
        await expect(modal).toBeVisible()

        // Set Primary Key for Patients -> patient_id
        // First select is PK (0-indexed)
        await modal.locator('select').nth(0).selectOption('patient_id')

        // Click Save for PK
        // Button next to PK select
        const pkSaveBtn = modal.locator('button', { hasText: 'Save' }).first()
        await pkSaveBtn.click()

        // Verify PK is saved (button might disable or change text, but let's assume it persists)
        // Close modal
        await modal.getByRole('button', { name: '×' }).click()

        // Verify PK is shown on card
        await expect(patientsCard.getByText('PK: patient_id')).toBeVisible()

        // Now add relationship from Treatments to Patients
        const treatmentsCard = page.getByTestId('table-card-treatments')
        await treatmentsCard.getByRole('button', { name: 'Manage Keys' }).click()

        // Wait for modal
        const treatmentsModal = page.locator('h3', { hasText: 'Manage Keys & Relationships' }).locator('xpath=../..')
        await expect(treatmentsModal).toBeVisible()

        // Check "Relationships" section and verify no existing relationships (optional)

        // Add relationship: Treatments.patient_id -> Patients.patient_id
        // Selects are:
        // 1: FK Column (treatments column) -> patient_id
        // 2: Ref Table -> Patients
        // 3: Ref Column -> patient_id
        // Note: index 0 is PK select

        await treatmentsModal.locator('select').nth(1).selectOption('patient_id')

        // Select Ref Table by label text "Patients"
        await treatmentsModal.locator('select').nth(2).selectOption({ label: 'Patients' })

        // Select Ref Column
        // Need to wait for ref columns to load? Maybe.
        // Let's assume it's fast enough or handled by reactivity.
        await treatmentsModal.locator('select').nth(3).selectOption('patient_id')

        // Click Add Relationship button
        await treatmentsModal.getByRole('button', { name: 'Add Relationship' }).click()

        // Verify it appears in the list
        // "patient_id → Patients.patient_id"
        await expect(treatmentsModal.getByText('patient_id → Patients.patient_id')).toBeVisible()

        // Close modal
        await treatmentsModal.getByRole('button', { name: '×' }).click()

        // Verify on card
        await expect(treatmentsCard.getByText('patient_id → Patients.patient_id')).toBeVisible()
    })
})

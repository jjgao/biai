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
      await apiHelpers.deleteDataset(datasetId).catch(() => {})
    }
  })

  test('rename table', async ({ page }) => {
    await page.goto(`/datasets/${datasetId}/manage`)
    await page.waitForLoadState('domcontentloaded')

    // Wait for page to load
    await expect(page.getByRole('button', { name: 'Delete Dataset' })).toBeVisible()
    await expect(page.getByText('Geographic Data')).toBeVisible()

    // Locate the table card
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

    const tableCard = page.getByTestId('table-card-geographic')

    // Click Manage Columns within that card
    await tableCard.getByRole('button', { name: 'Manage Columns' }).click()

    // Modal should appear
    await expect(page.getByRole('heading', { name: 'Manage Column Metadata' })).toBeVisible()

    // Find card for column "state"
    const colCard = page.getByTestId('column-card-state')
    await expect(colCard).toBeVisible()

    // Edit Display Name
    const displayNameInput = colCard.locator('input[type="text"]').first()

    const updateNamePromise = page.waitForResponse(resp =>
      resp.url().includes('/columns/state') &&
      resp.request().method() === 'PATCH' &&
      resp.status() === 200
    )

    await displayNameInput.fill('State Name')
    await displayNameInput.blur()
    await updateNamePromise

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

    const tableCardAfterReload = page.getByTestId('table-card-geographic')
    await expect(tableCardAfterReload).toBeVisible()
    await tableCardAfterReload.getByRole('button', { name: 'Manage Columns' }).click()

    await expect(page.getByRole('heading', { name: 'Manage Column Metadata' })).toBeVisible()

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
    await modal.locator('select').nth(0).selectOption('patient_id')

    // Click Save for PK
    const pkSaveBtn = modal.locator('button', { hasText: 'Save' }).first()
    await pkSaveBtn.click()

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

    // Add relationship: Treatments.patient_id -> Patients.patient_id
    // Selects: 0=PK, 1=FK Column, 2=Ref Table, 3=Ref Column
    await treatmentsModal.locator('select').nth(1).selectOption('patient_id')
    await treatmentsModal.locator('select').nth(2).selectOption({ label: 'Patients' })
    await treatmentsModal.locator('select').nth(3).selectOption('patient_id')

    // Click Add Relationship button
    await treatmentsModal.getByRole('button', { name: 'Add Relationship' }).click()

    // Verify it appears in the list
    await expect(treatmentsModal.getByText('patient_id → Patients.patient_id')).toBeVisible()

    // Close modal
    await treatmentsModal.getByRole('button', { name: '×' }).click()

    // Verify on card
    await expect(treatmentsCard.getByText('patient_id → Patients.patient_id')).toBeVisible()
  })
})

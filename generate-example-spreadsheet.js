const fs = require('fs')
const path = require('path')
let XLSX
try {
  XLSX = require('xlsx')
} catch (e) {
  try {
    XLSX = require('./node_modules/xlsx')
  } catch (e2) {
    console.error('Could not find xlsx module. Please run "npm install" first.')
    process.exit(1)
  }
}

/**
 * Generate an example multi-sheet spreadsheet for testing
 */
function generateExampleSpreadsheet(outputDir) {
  try {
    console.log(`
📊 Generating example spreadsheet in: ${outputDir}
`)

    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true })
    }

    const wb = XLSX.utils.book_new()

    // Sheet 1: Patients
    console.log('   Creating sheet: Patients')
    const patientsData = [
      ['patient_id', 'age', 'gender', 'diagnosis_date', 'status'],
      ['P001', 45, 'Male', '2023-01-15', 'Alive'],
      ['P002', 32, 'Female', '2023-02-20', 'Deceased'],
      ['P003', 67, 'Male', '2023-03-10', 'Alive'],
      ['P004', 54, 'Female', '2023-04-05', 'Alive'],
      ['P005', 29, 'Female', '2023-05-12', 'Alive']
    ]
    const ws1 = XLSX.utils.aoa_to_sheet(patientsData)
    XLSX.utils.book_append_sheet(wb, ws1, 'Patients')

    // Sheet 2: Samples
    console.log('   Creating sheet: Samples')
    const samplesData = [
      ['sample_id', 'patient_id', 'sample_type', 'tissue_site', 'purity'],
      ['S001', 'P001', 'Primary Tumor', 'Brain', 0.85],
      ['S002', 'P001', 'Normal Blood', 'Blood', 1.0],
      ['S003', 'P002', 'Primary Tumor', 'Brain', 0.78],
      ['S004', 'P003', 'Recurrent Tumor', 'Brain', 0.92],
      ['S005', 'P004', 'Primary Tumor', 'Brain', 0.65],
      ['S006', 'P005', 'Metastatic', 'Lung', 0.70]
    ]
    const ws2 = XLSX.utils.aoa_to_sheet(samplesData)
    XLSX.utils.book_append_sheet(wb, ws2, 'Samples')

    // Sheet 3: Treatments (with some empty rows to test skip logic)
    console.log('   Creating sheet: Treatments')
    const treatmentsData = [
      ['treatment_id', 'patient_id', 'drug_name', 'response'],
      ['T001', 'P001', 'Temozolomide', 'Stable Disease'],
      ['T002', 'P002', 'Bevacizumab', 'Progressive Disease'],
      ['T003', 'P003', 'Temozolomide', 'Complete Response'],
      ['T004', 'P004', 'Carboplatin', 'Partial Response']
    ]
    const ws3 = XLSX.utils.aoa_to_sheet(treatmentsData)
    XLSX.utils.book_append_sheet(wb, ws3, 'Treatments')

    // Sheet 4: Empty Sheet (to test error handling)
    console.log('   Creating sheet: Empty_Sheet')
    const ws4 = XLSX.utils.aoa_to_sheet([])
    XLSX.utils.book_append_sheet(wb, ws4, 'Empty_Sheet')

    const outputPath = path.join(outputDir, 'clinical_trial_data.xlsx')
    XLSX.writeFile(wb, outputPath)

    console.log(`
✅ Successfully created: ${outputPath}`)
    console.log(`
You can now upload this file using the "Add Table" -> "Upload File" feature in BIAI.
`)

  } catch (error) {
    console.error('\n❌ Generation failed:', error.message)
    process.exit(1)
  }
}

// Run with command line argument
const outputDir = process.argv[2] || 'example_data'
generateExampleSpreadsheet(outputDir)

/**
 * Quick script to test temporal SQL generation without UI
 * Run with: npx tsx src/scripts/test-temporal-sql.ts
 */

import aggregationService from '../services/aggregationService.js'

// Access private method for testing
const callPrivate = <T extends (...args: any[]) => any>(fnName: string) => {
  const fn = (aggregationService as unknown as Record<string, T>)[fnName]
  return fn.bind(aggregationService)
}

const buildFilterCondition = callPrivate('buildFilterCondition')

console.log('=== Testing Temporal SQL Generation ===\n')

// Test 1: temporal_before
console.log('1. temporal_before (sample_date before treatment_start):')
const beforeFilter = {
  column: 'sample_date',
  operator: 'temporal_before' as const,
  temporal_reference_column: 'treatment_start'
}
console.log('Filter:', JSON.stringify(beforeFilter, null, 2))
console.log('SQL:', buildFilterCondition(beforeFilter))
console.log()

// Test 2: temporal_after
console.log('2. temporal_after (followup_date after treatment_end):')
const afterFilter = {
  column: 'followup_date',
  operator: 'temporal_after' as const,
  temporal_reference_column: 'treatment_end'
}
console.log('Filter:', JSON.stringify(afterFilter, null, 2))
console.log('SQL:', buildFilterCondition(afterFilter))
console.log()

// Test 3: temporal_duration
console.log('3. temporal_duration (treatment duration >= 180 days):')
const durationFilter = {
  column: 'treatment_start',
  operator: 'temporal_duration' as const,
  temporal_reference_column: 'treatment_end',
  value: 180
}
console.log('Filter:', JSON.stringify(durationFilter, null, 2))
console.log('SQL:', buildFilterCondition(durationFilter))
console.log()

// Test 4: Complex AND filter
console.log('4. Complex filter (treatment A AND before event B):')
const complexFilter = {
  and: [
    {
      column: 'treatment_type',
      operator: 'eq' as const,
      value: 'Chemotherapy'
    },
    {
      column: 'treatment_start',
      operator: 'temporal_before' as const,
      temporal_reference_column: 'progression_date'
    }
  ]
}
console.log('Filter:', JSON.stringify(complexFilter, null, 2))
console.log('SQL:', buildFilterCondition(complexFilter))
console.log()

console.log('=== All tests completed successfully! ===')

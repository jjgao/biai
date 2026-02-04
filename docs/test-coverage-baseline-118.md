# DatasetExplorer Test Coverage Baseline (Issue #118)

**Date:** 2026-02-04  
**Status:** Phase 0 Complete  
**Total Tests:** 83 passing

## Summary

This document establishes the baseline test coverage for `DatasetExplorer.tsx` before beginning the refactoring work outlined in Epic #92.

## Test Breakdown

### Integration Tests (27 tests)
**File:** `client/src/pages/__tests__/DatasetExplorer.test.tsx`

- **Smoke Test** (1 test)
  - Basic rendering with mock dataset

- **Filter Persistence** (6 tests)
  - Saves/loads filters to/from localStorage
  - Updates URL hash when filters change
  - Restores filters from URL hash on mount
  - Handles NOT-wrapped filters correctly
  - Handles nested NOT with OR filters
  - Serializes NOT filters with countByKey metadata

- **Filter Presets** (5 tests)
  - Saves new filter preset to localStorage
  - Loads and applies filter presets
  - Deletes filter presets
  - Applying preset restores count-by selections
  - Applying preset with NOT filters preserves NOT wrapper

- **View Preferences** (1 test)
  - Toggles between chart and table view with localStorage persistence

- **Count By Controls** (3 tests)
  - Shows multi-hop parent options and renders ancestor badges
  - Reuses cached aggregations when toggling count targets
  - Shows pie percentage toggle for parent metrics

- **Dashboard Integration** (2 tests)
  - Pins charts with the selected count-by target
  - Dashboard chart tooltips include ancestor path

- **Filter Migration Utilities** (1 test)
  - Migrates legacy filters to include row count keys

- **Chart Override Persistence** (2 tests)
  - Persist/load helpers round-trip overrides via storage
  - Loads overrides from localStorage and requests ancestor aggregations

- **API Error Handling** (2 tests)
  - Renders error message when dataset fetch fails
  - Renders error message when columns fetch fails

- **Complex Filter Queries** (1 test)
  - Constructs correct API params for nested complex filters

- **Loading States** (1 test)
  - Shows loading indicator while fetching dataset

- **Interaction Tests** (2 tests)
  - Switches table tabs correctly (Dashboard → Customers → Orders)
  - Renders correct chart types based on metadata

### Unit Tests (56 tests)

**file:** `client/src/utils/__tests__/filterHelpers.test.ts` (47 tests)
- Filter column/table extraction
- Column containment checks
- Relationship path finding
- Effective filters calculation
- NOT filter unwrapping
- Range key generation and comparison
- Filter count key generation

**File:** `client/src/utils/__tests__/urlHelpers.test.ts` (9 tests)
- State encoding/decoding
- Round-trip serialization
- Error handling for invalid JSON
- Handling of circular references

**File:** `client/src/utils/__tests__/presetHelpers.test.ts` (9 tests - not included in count above, separate run)
- Save/load presets to/from localStorage
- Create new preset with deep cloning
- Normalize imported presets
- Migrate legacy filters on load
- Error handling

## Coverage Areas

### ✅ Fully Covered
- Filter logic (apply, remove, propagate across tables)
- URL state encoding/decoding (hash-based filter sharing)
- Preset save/load/import/export
- Table tab switching
- Cross-table relationship handling
- Edge cases (empty data, error states)
- Chart type selection based on metadata

### ⚠️ Partially Covered
- Chart rendering (verified chart type selection, but not all data shapes)
- Ancestor selection (verified multi-hop paths, but not all edge cases)

### ❌ Not Covered
- Large dataset performance testing (deferred to e2e tests)
- Visual regression testing for charts (requires different tooling)
- Accessibility testing (future work)

## Extracted Utilities

The following pure logic has been extracted to standalone modules with unit tests:

1. **`client/src/utils/urlHelpers.ts`**
   - `encodeState()` - URL-safe state encoding
   - `decodeState()` - URL state decoding with error handling

2. **`client/src/utils/filterHelpers.ts`**
   - `getFilterColumn()` - Extract column from filter
   - `getFilterTableName()` - Extract table from filter
   - `filterContainsColumn()` - Check if filter applies to column
   - `findRelationshipPath()` - Find path between tables
   - `tablesHaveRelationship()` - Check table relationship
   - `getAllEffectiveFilters()` - Flatten complex filters
   - `unwrapNot()` - Extract filter from NOT wrapper
   - `rangeKey()` - Generate cache key for range filters
   - `rangesEqual()` - Compare range definitions
   - `getFilterCountKey()` - Get count-by key from filter
   - `migrateFiltersToCurrentSchema()` - Add missing countByKey to legacy filters

3. **`client/src/utils/presetHelpers.ts`**
   - `savePresetsToLocalStorage()` - Persist presets
   - `loadPresetsFromLocalStorage()` - Load and migrate presets
   - `createNewPreset()` - Create preset with deep cloning
   - `normalizeImportedPresets()` - Validate and normalize imports

## Related PRs

- **PR #125**: Extract and test URL & filter helpers
- **PR #126**: Extract and test preset helpers

## Next Steps

With this baseline established, we can now proceed with:
1. Phase 1: Extract hooks from DatasetExplorer (#119)
2. Phase 2: Extract chart components (#120)
3. Phase 3: Extract filter components (#121)
4. Phase 4: Extract utility components (#122)
5. Phase 5: Testing & polish (#123)

## Notes

- All tests pass consistently (83/83)
- Test execution time: ~4.3 seconds
- No flaky tests identified
- Mock data structure matches production schema

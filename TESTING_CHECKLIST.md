# Dataset Explorer Testing Checklist

## Purpose
This checklist ensures no regressions occur when refactoring the Dataset Explorer layout from sections to tabs, and when adding the dashboard feature.

> **Note:** The "Before Refactor" section reflects the current UI as of v0.1. The "After Phase 1" and "After Phase 2" sections describe planned features from the DatasetExplorer refactoring epic (#92, #119-#123) and are aspirational.

---

## Before Refactor (Baseline) - Section Layout

### Data Loading
- [ ] Dataset name and description display correctly
- [ ] All tables render with correct row counts
- [ ] All columns with charts are visible
- [ ] Hidden columns do not appear
- [ ] Loading state shows while fetching data
- [ ] Error states handled gracefully

### Chart Rendering
- [ ] Pie charts render for categorical columns (≤8 categories)
- [ ] Bar charts render for categorical columns (>8 categories, if chart view selected)
- [ ] Table view renders for categorical columns (>8 categories, by default)
- [ ] Histograms render for numeric columns
- [ ] All charts display correct data
- [ ] Chart colors match table color coding

### Chart Interactions - Categorical (Pie/Bar)
- [ ] Click on pie chart segment adds filter
- [ ] Click on bar chart bar adds filter
- [ ] Multiple clicks on same segment/bar adds to existing filter (OR logic)
- [ ] Charts update immediately after filter is added
- [ ] Filter badge appears showing the filter

### Chart Interactions - Numeric (Histogram)
- [ ] Click on histogram bar adds range filter
- [ ] Click additional bars adds to range selection
- [ ] "Apply Ranges" button appears after selection
- [ ] "Clear Selection" button clears range selection
- [ ] Custom range inputs work (min/max)
- [ ] Custom range "Apply" button adds filter
- [ ] Histogram updates after range filter applied

### Chart Interactions - Table View
- [ ] Table view shows all categories with counts
- [ ] Click on table row adds filter
- [ ] Table updates after filter applied
- [ ] Pagination works for large category lists (if implemented)

### Filter Management
- [ ] New filter appears in "Active Filters" section
- [ ] Filter color matches source table
- [ ] Filter shows correct operator and value
- [ ] "Remove" (×) button deletes individual filter
- [ ] "Clear All" button removes all filters
- [ ] All charts update when filter removed

### Filter Propagation (Cross-Table)
- [ ] Direct filter on table shows blue badge (e.g., "2 filters")
- [ ] Related tables show propagated filter badge (e.g., "+1 linked")
- [ ] Transitive relationships show hop count (e.g., "+1 linked (2-hop)")
- [ ] Filtered row count updates correctly
- [ ] Percentage badge shows correct filtered %
- [ ] Unrelated tables show no propagated filters

### Filter Presets
- [ ] "Save Filter" button appears when filters exist
- [ ] Save preset dialog accepts name input
- [ ] Saved preset appears in "Load Filter" dropdown
- [ ] "Manage" button opens preset management dialog
- [ ] Load preset applies filters correctly
- [ ] Rename preset works
- [ ] Delete preset removes from list
- [ ] Export presets downloads JSON file
- [ ] Import presets loads from JSON file

### Persistence (localStorage)
- [ ] Filters persist to localStorage when changed
- [ ] Filters restore from localStorage on page load
- [ ] Presets persist to localStorage
- [ ] Presets restore from localStorage on page load
- [ ] View preferences persist (chart/table toggle)
- [ ] View preferences restore on page load

### Persistence (URL Hash)
- [ ] URL hash updates when filters added
- [ ] URL hash clears when all filters removed
- [ ] Filters restore from URL hash on page load
- [ ] Shareable URL preserves filters
- [ ] Browser back/forward respects filter state

### View Preferences
- [ ] Toggle button switches between chart and table view
- [ ] Toggle shows "📊 Chart" or "📋 Table" appropriately
- [ ] View preference persists after page reload
- [ ] Default view is correct (table for >8 categories)

### Table Headers
- [ ] Table name displays correctly
- [ ] Table color bar matches assigned color
- [ ] Row count shows (filtered / total) when filtered
- [ ] Percentage badge shows correct % when filtered
- [ ] Column count is accurate
- [ ] Filter badges show correct counts
- [ ] Table code name badge displays

### Visual/Layout
- [ ] Grid layout displays charts correctly (175px tiles)
- [ ] Categorical charts span 1 column × 1 row
- [ ] Bar charts and histograms span 2 columns × 1 row
- [ ] Table views span 2 columns × 2 rows
- [ ] No overflow or layout breaking
- [ ] Colors are consistent and accessible

### NOT Operator Support
- [ ] NOT toggle button appears on filters
- [ ] Clicking NOT inverts the filter logic
- [ ] NOT filter shows visual indicator (strikethrough)
- [ ] Charts update correctly with NOT filter
- [ ] NOT operator persists in localStorage/URL

---

## After Phase 1 (Tab Layout)

### Tab Navigation
- [ ] Tab bar appears at top with all table names
- [ ] Active tab is visually highlighted
- [ ] Click on tab switches to that table
- [ ] Only active tab content is visible
- [ ] Tab shows table display name (not code name)
- [ ] Tabs are color-coded to match tables
- [ ] First tab is selected by default

### All Previous Features Still Work
**Run through entire "Before Refactor" checklist above**

Key areas to verify:
- [ ] Chart interactions still add filters
- [ ] Filter propagation still works across tables
- [ ] Switching tabs shows correct filtered data
- [ ] Filter badges visible on non-active tabs (if applicable)
- [ ] All persistence (localStorage, URL) still works
- [ ] No console errors
- [ ] No visual regressions

---

## After Phase 2 (Dashboard Tab) - Future

### Dashboard Tab
- [ ] "Dashboard" tab appears as first tab
- [ ] Dashboard starts empty initially
- [ ] Empty state message shows helpful text

### Adding Charts to Dashboard
- [ ] "Add to Dashboard" button appears on each chart
- [ ] Button shows "✓" when chart is on dashboard
- [ ] Click toggles chart on/off dashboard
- [ ] "Add All Charts" button adds all from table
- [ ] Charts appear on dashboard in chronological order

### Dashboard Display
- [ ] Dashboard uses same grid layout as table tabs
- [ ] Charts show table name in title
- [ ] Charts have color-coded borders
- [ ] Remove (×) button appears on hover
- [ ] Click remove button removes from dashboard
- [ ] "Clear Dashboard" button removes all

### Dashboard Behavior
- [ ] Dashboard charts update with active filters (live)
- [ ] Click on dashboard chart adds filter
- [ ] Dashboard persists to localStorage
- [ ] Dashboard restores on page reload
- [ ] Dashboard count shows in tab label

### Dashboard + Filters
- [ ] Filter count badge shows on dashboard tab
- [ ] Dashboard charts show filtered data
- [ ] Filter from dashboard chart propagates correctly
- [ ] Removing filter updates dashboard charts

---

## Browser Compatibility
- [ ] Chrome (latest)
- [ ] Firefox (latest)
- [ ] Safari (latest)
- [ ] Edge (latest)

---

## Performance
- [ ] Dataset with 5 tables loads in <3 seconds
- [ ] Dataset with 10+ tables loads without hanging
- [ ] Tab switching is immediate (<100ms)
- [ ] Filter application updates charts in <1 second
- [ ] No memory leaks (check dev tools)

---

## Notes
- Document any issues found during testing
- Include screenshots for visual bugs
- Note browser/OS if issue is environment-specific

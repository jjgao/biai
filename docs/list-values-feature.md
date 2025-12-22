# List Values Feature Guide

## Overview

The application now supports list/array values in CSV files. List columns are automatically detected, parsed, and displayed as expanded items in pie charts with full filtering support.

## Supported Formats

**Python List Syntax:**
```
['Gene expression analysis', 'Copy number analysis', 'Survival analysis']
```

**JSON Array Syntax:**
```
["Gene expression analysis", "Copy number analysis", "Survival analysis"]
```

## How It Works

### 1. Auto-Detection During Upload

When you preview a CSV file, the system automatically detects columns that contain list values:

```bash
POST /api/datasets/:id/tables/preview
```

Response includes:
```json
{
  "listSuggestions": [
    {
      "columnName": "analysis_type",
      "confidence": "high",
      "listSyntax": "python",
      "avgItemCount": 2.5,
      "uniqueItemCount": 15,
      "hasNestedLists": false
    }
  ]
}
```

**Confidence Levels:**
- **High**: 90%+ of values match list pattern
- **Medium**: 70-90% of values match list pattern
- **Low**: 50-70% (not suggested)

### 2. Upload with List Configuration

When uploading, specify which columns should be treated as lists:

```bash
POST /api/datasets/:id/tables
-F "file=@data.csv"
-F "tableName=my_table"
-F 'listColumns={"analysis_type":"python","tags":"json"}'
```

### 3. Data Storage

List columns are stored as ClickHouse `Array(String)` columns:

```sql
CREATE TABLE my_table (
  id String,
  analysis_type Nullable(Array(String)),
  ...
)
```

### 4. Visualization

**Before (without list support):**
- Pie chart shows: `"['Item1', 'Item2']": 5 rows`
- Each unique list string is one category

**After (with list support):**
- Pie chart shows:
  - `Item1: 8 occurrences`
  - `Item2: 6 occurrences`
  - `Item3: 4 occurrences`
- Each item is a separate category

### 5. Filtering

Click on any list item to filter rows containing that item:

```sql
-- Generated query uses has() function
SELECT * FROM table WHERE has(analysis_type, 'Gene expression analysis')
```

Multi-select works with OR logic:
```sql
WHERE has(analysis_type, 'Item1') OR has(analysis_type, 'Item2')
```

## Database Migration

For existing databases, run this migration:

```sql
USE biai;

ALTER TABLE dataset_columns
ADD COLUMN IF NOT EXISTS is_list_column Boolean DEFAULT false;

ALTER TABLE dataset_columns
ADD COLUMN IF NOT EXISTS list_syntax String DEFAULT '';
```

## Example: cBioPortal Citations Dataset

The `example_data/cbioportal-citations.csv` file contains list columns:

- `analysis_type`: `['Gene expression analysis', 'Copy number analysis', ...]`
- `cancer_type`: `['Lung cancer', 'Breast cancer']`
- `specific_genes_queried`: `['TP53', 'EGFR', 'KRAS']`

Upload command:
```bash
curl -X POST http://localhost:3001/api/datasets/{dataset-id}/tables \
  -F "file=@example_data/cbioportal-citations.csv" \
  -F "tableName=cbioportal_citations" \
  -F "delimiter=," \
  -F 'listColumns={"analysis_type":"python","cancer_type":"python","specific_genes_queried":"python"}'
```

## Edge Cases Handled

- **Empty lists**: `[]` → stored as empty array
- **Null values**: Handled gracefully
- **Malformed strings**: Logged and stored as NULL
- **Nested lists**: Detected but rejected (not supported in v1)
- **Mixed syntax**: Auto-detection handles both formats
- **Large arrays**: Parsed without size limits

## Performance Considerations

### ARRAY JOIN Performance
- Expands rows: N rows × M items = N×M intermediate rows
- Filters applied before ARRAY JOIN when possible
- Use WHERE clauses to reduce data before expansion

### Indexing
For better performance on large datasets, add bloom filter indexes:

```sql
ALTER TABLE my_table
ADD INDEX analysis_type_bloom analysis_type
TYPE bloom_filter GRANULARITY 1;
```

This speeds up `has()` queries by ~10-100x on large datasets.

## API Reference

### Preview Endpoint
```
POST /api/datasets/:id/tables/preview
```

**Request:**
- `file`: CSV file upload
- `skipRows`: Number of metadata rows to skip
- `delimiter`: Column delimiter (default: tab)

**Response:**
```json
{
  "preview": {
    "columns": [...],
    "sampleRows": [...],
    "listSuggestions": [
      {
        "columnName": "tags",
        "confidence": "high",
        "listSyntax": "python",
        "avgItemCount": 3.2,
        "uniqueItemCount": 50,
        "hasNestedLists": false
      }
    ]
  }
}
```

### Upload Endpoint
```
POST /api/datasets/:id/tables
```

**Request:**
- `file`: CSV file upload
- `tableName`: Table name
- `delimiter`: Column delimiter
- `listColumns`: JSON object mapping column names to syntax
  - Example: `{"tags":"python","categories":"json"}`

## Frontend Integration (TODO)

To complete the feature, the frontend needs:

1. **Upload UI**: Show list suggestions during preview
   - Display suggested columns with confidence badges
   - Checkboxes to enable/disable each suggestion
   - Dropdown to override detected syntax (Python/JSON)

2. **Visual Indicators**: Add list icon next to column names
   - Badge or icon (📋) on pie chart headers
   - Tooltip: "List column - items can appear in multiple rows"

3. **Settings**: Allow converting existing columns
   - Detect lists in existing datasets
   - Convert string columns to array columns
   - Re-parse and update data

## Related Files

- **Parser**: `server/src/utils/listParser.ts`
- **Detection**: `server/src/services/columnAnalyzer.ts` (line 364-474)
- **Aggregation**: `server/src/services/aggregationService.ts` (ARRAY JOIN)
- **Filtering**: `server/src/services/aggregationService.ts` (has() function)
- **API Routes**: `server/src/routes/datasets.ts`
- **Schema**: `clickhouse/init/01-init.sql`

## Troubleshooting

### Lists not detected
- Check if values match `['...']` or `["..."]` pattern
- Ensure >50% of values are list-formatted
- Verify quotes are consistent (all single or all double)

### Parse errors
- Check for unescaped quotes in items
- Ensure brackets are balanced
- Look for nested lists (not supported)

### Filtering not working
- Verify column is marked as `is_list_column=true`
- Check ClickHouse column type is `Array(String)`
- Ensure list items don't have leading/trailing spaces

### Performance issues
- Add bloom filter indexes on list columns
- Apply filters before ARRAY JOIN when possible
- Consider limiting results with TOP N queries

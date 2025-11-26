#!/bin/bash
# Test temporal filtering via API
# Usage: ./test-temporal-api.sh <datasetId> <tableId>

DATASET_ID=${1:-"your-dataset-id"}
TABLE_ID=${2:-"your-table-id"}
BASE_URL="http://localhost:3000"

echo "Testing temporal filtering API endpoints..."
echo "Dataset: $DATASET_ID"
echo "Table: $TABLE_ID"
echo ""

# Test 1: temporal_before
echo "=== Test 1: temporal_before ==="
FILTER='[{"column":"sample_date","operator":"temporal_before","temporal_reference_column":"treatment_start","tableName":"samples"}]'
ENCODED_FILTER=$(echo -n "$FILTER" | base64)
echo "Filter: $FILTER"
echo "Calling: GET /api/datasets/$DATASET_ID/tables/$TABLE_ID/aggregations"
curl -s "$BASE_URL/api/datasets/$DATASET_ID/tables/$TABLE_ID/aggregations?filters=$ENCODED_FILTER" | jq '.' 2>/dev/null || echo "Error: jq not installed or API returned error"
echo ""

# Test 2: temporal_after
echo "=== Test 2: temporal_after ==="
FILTER='[{"column":"followup_date","operator":"temporal_after","temporal_reference_column":"treatment_end","tableName":"patients"}]'
ENCODED_FILTER=$(echo -n "$FILTER" | base64)
echo "Filter: $FILTER"
curl -s "$BASE_URL/api/datasets/$DATASET_ID/tables/$TABLE_ID/aggregations?filters=$ENCODED_FILTER" | jq '.' 2>/dev/null || echo "Error: jq not installed or API returned error"
echo ""

# Test 3: temporal_duration
echo "=== Test 3: temporal_duration (treatment >= 180 days) ==="
FILTER='[{"column":"treatment_start","operator":"temporal_duration","temporal_reference_column":"treatment_end","value":180,"tableName":"treatments"}]'
ENCODED_FILTER=$(echo -n "$FILTER" | base64)
echo "Filter: $FILTER"
curl -s "$BASE_URL/api/datasets/$DATASET_ID/tables/$TABLE_ID/aggregations?filters=$ENCODED_FILTER" | jq '.' 2>/dev/null || echo "Error: jq not installed or API returned error"
echo ""

echo "Done! Check server logs to see generated SQL queries."

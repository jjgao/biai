-- Migration: Add list column support to dataset_columns table
-- This adds fields to track which columns contain list/array values

ALTER TABLE biai.dataset_columns
ADD COLUMN IF NOT EXISTS is_list_column Boolean DEFAULT false;

ALTER TABLE biai.dataset_columns
ADD COLUMN IF NOT EXISTS list_syntax String DEFAULT '';

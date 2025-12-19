-- Migration: Add temporal filtering columns to dataset_columns table
-- Date: 2025-01-26
-- Related Issue: #77 (Phase 1: Backend foundation for temporal filtering)

USE biai;

-- Add temporal_role column
ALTER TABLE dataset_columns
ADD COLUMN IF NOT EXISTS temporal_role Enum8('none' = 0, 'start_date' = 1, 'stop_date' = 2, 'duration' = 3) DEFAULT 'none';

-- Add temporal_paired_column column (for linking start/stop date pairs)
ALTER TABLE dataset_columns
ADD COLUMN IF NOT EXISTS temporal_paired_column Nullable(String);

-- Add temporal_unit column (for interpreting numeric values as days/months/years)
ALTER TABLE dataset_columns
ADD COLUMN IF NOT EXISTS temporal_unit Enum8('days' = 0, 'months' = 1, 'years' = 2) DEFAULT 'days';

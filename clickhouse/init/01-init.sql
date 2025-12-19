-- Create database if not exists
CREATE DATABASE IF NOT EXISTS biai;

-- Use the database
USE biai;

-- Datasets metadata (container for multiple tables)
CREATE TABLE IF NOT EXISTS datasets_metadata (
    dataset_id String,
    dataset_name String,
    description String,
    tags Array(String) DEFAULT [],
    source String DEFAULT '',
    citation String DEFAULT '',
    references Array(String) DEFAULT [],
    custom_metadata String DEFAULT '{}',
    connection_settings String DEFAULT '',
    created_by String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (created_at, dataset_id);

-- Individual tables within datasets
CREATE TABLE IF NOT EXISTS dataset_tables (
    dataset_id String,
    table_id String,
    table_name String,
    display_name String,
    original_filename String,
    file_type String,
    row_count UInt64,
    clickhouse_table_name String,
    schema_json String,
    primary_key Nullable(String),
    custom_metadata String DEFAULT '{}',
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (dataset_id, created_at);

-- Column metadata for each table
CREATE TABLE IF NOT EXISTS dataset_columns (
    dataset_id String,
    table_id String,
    column_name String,
    column_type String,
    column_index UInt32,
    is_nullable Boolean,
    display_name String DEFAULT '',
    description String DEFAULT '',
    user_data_type String DEFAULT '',
    user_priority Nullable(Int32),
    display_type String DEFAULT 'auto',
    unique_value_count UInt32 DEFAULT 0,
    null_count UInt32 DEFAULT 0,
    min_value Nullable(String),
    max_value Nullable(String),
    suggested_chart String DEFAULT '',
    display_priority Int32 DEFAULT 0,
    is_hidden Boolean DEFAULT false,
    is_list_column Boolean DEFAULT false,
    list_syntax String DEFAULT '',
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (dataset_id, table_id, column_index);

-- Table relationships (foreign keys)
CREATE TABLE IF NOT EXISTS table_relationships (
    dataset_id String,
    table_id String,
    foreign_key String,
    referenced_table String,
    referenced_column String,
    relationship_type String DEFAULT 'many-to-one',
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (dataset_id, table_id);

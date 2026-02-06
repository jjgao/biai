# BIAI - Business Intelligence AI

[![Test & Build](https://github.com/jjgao/biai/actions/workflows/test.yml/badge.svg)](https://github.com/jjgao/biai/actions/workflows/test.yml)

A modern BI tool built with React, Node.js, ClickHouse, and Recharts.

## Tech Stack

- **Frontend**: React + Vite + TypeScript
- **Backend**: Node.js + Express + TypeScript
- **Database**: ClickHouse
- **Visualization**: Plotly.js

## Getting Started

### Prerequisites

- Node.js >= 18.0.0
- Docker and Docker Compose
- npm or yarn

### Installation

1. Install dependencies:
```bash
npm install
```

2. Set up environment variables:
```bash
cp server/.env.example server/.env
```

3. Start ClickHouse:
```bash
docker-compose up -d
```

4. Start development servers:
```bash
npm run dev
```

This will start:
- Frontend at http://localhost:3000
- Backend at http://localhost:5001

### Project Structure

```
biai/
├── client/                     # React frontend
│   ├── src/
│   │   ├── pages/             # Dataset management, Dashboard, Reports
│   │   ├── services/          # API client
│   │   └── main.tsx           # App entry point
│   └── package.json
├── server/                     # Node.js backend
│   ├── src/
│   │   ├── routes/            # API endpoints (datasets, queries)
│   │   ├── services/          # Business logic (datasetService, fileParser, metadataParser)
│   │   └── config/            # ClickHouse configuration
│   └── package.json
├── clickhouse/                 # ClickHouse initialization scripts
│   └── init/01-init.sql       # Database schema
├── example_data/               # Sample TCGA clinical data with .meta files
└── docker-compose.yml          # Docker services
```

## User Documentation

For end-users working with the web interface:

- **[User Guide](docs/USER_GUIDE.md)**: Comprehensive guide to exploring and analyzing data
- **[Quick Reference](docs/QUICK_REFERENCE.md)**: At-a-glance guide for common tasks
- **[FAQ](docs/FAQ.md)**: Answers to frequently asked questions

## Features

- **Multi-table Datasets**: Create datasets with multiple related tables
- **Metadata-Driven Uploads**: Configure datasets using `.meta` files with support for:
  - Dataset metadata (name, description, tags, source, citation, references)
  - Table metadata (primary keys, relationships, custom fields)
  - Nested object and array formats
- **Table Relationships**: Define and track foreign key relationships between tables
- **Dynamic Schema**: Automatic type inference from CSV/TSV files
- **File Upload**: Support for CSV, TSV with configurable delimiters
- **List/Array Values**: Full support for list columns in CSV files
  - Auto-detection of list columns (Python `['item1', 'item2']` and JSON `["item1", "item2"]` syntax)
  - Stores as ClickHouse Array(String) columns for efficient querying
  - Visualize individual list items in pie charts
  - Filter by individual items with multi-select OR logic
- **Auto-Delimiter Detection**: Automatically detects CSV delimiters (comma, tab, semicolon, pipe) with visual feedback
- **Data Preview**: View table data with pagination
- **Custom Metadata**: Store domain-specific fields alongside standard metadata
- **Interactive Data Exploration**: Filter and visualize data with automatic chart selection
- **Relationship-Aware Filtering**: Filters automatically propagate across related tables
- **Filter Presets**: Save and share common filter combinations
- **URL-Based Sharing**: Share filtered views via URL parameters

## Available Scripts

- `npm run dev` - Start both frontend and backend in development mode
- `npm run dev:client` - Start frontend only
- `npm run dev:server` - Start backend only
- `npm run build` - Build both frontend and backend
- `npm test` - Run all tests
- `npm test --workspace=client` - Run client tests (105 tests)
- `npm test --workspace=server` - Run server tests (252 tests)

## API Endpoints

### Datasets
- `POST /api/datasets` - Create new dataset
- `GET /api/datasets` - List all datasets
- `GET /api/datasets/:id` - Get dataset details
- `DELETE /api/datasets/:id` - Delete dataset
- `POST /api/datasets/:id/tables` - Add table to dataset
- `GET /api/datasets/:id/tables/:tableId/data` - Get table data
- `DELETE /api/datasets/:id/tables/:tableId` - Delete table

### System
- `GET /health` - Health check

## Example Data

The project includes TCGA GBM clinical data with metadata in `example_data/gbm_tcga_pan_can_atlas_2018/`:
- `dataset.meta` - Dataset-level metadata (name, tags, citations, etc.)
- `data_clinical_patient.meta` - Table metadata for patients table
- `data_clinical_sample.meta` - Table metadata for samples table (includes relationship to patients)

Upload using the metadata-driven script:
```bash
node upload-dataset-with-metadata.js example_data/gbm_tcga_pan_can_atlas_2018
```

### Metadata File Format

Dataset metadata (`dataset.meta`):
```
name: Dataset Name
description: Dataset description
tags: tag1,tag2,tag3
source: Data source
citation: Citation info
references:
  - pmid:12345678
  - doi:10.1234/example
```

Table metadata (`table_name.meta`):
```
data_file: data_file.txt
table_name: table_name
display_name: Human Readable Name
skip_rows: 4
delimiter: tab
primary_key: id_column
relationship:
  foreign_key: foreign_id
  references_table: other_table
  references_column: id
  type: many-to-one
```

## Database Schema

The system uses ClickHouse with the following metadata tables:
- `datasets_metadata` - Dataset info (name, description, tags, source, citation, references, custom metadata)
- `dataset_tables` - Table info (name, row count, schema, primary key, custom metadata)
- `dataset_columns` - Column info (name, type, nullability, list column metadata)
- `table_relationships` - Foreign key relationships between tables

Data tables support Array(String) columns for storing list values.

## Implemented Features (v0.01)

- Multi-value selection from charts (OR logic within same column)
- AND/OR/NOT logic for filters in the UI
- Cross-table filtering with relationship propagation
- Filter presets (save/load/share)
- URL-based filter sharing
- Spreadsheet import (Excel, ODS)
- Database connectivity (connect to external ClickHouse)
- Survival curve analysis
- CI/CD pipeline with GitHub Actions

## Roadmap

- Chatbot integration for natural language queries
- Session management for query optimization
- Additional genomics data support
- Extended data source connectors


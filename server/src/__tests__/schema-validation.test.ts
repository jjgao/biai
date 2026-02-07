import { describe, test, expect } from 'vitest'
import fs from 'fs'
import path from 'path'

const INIT_SQL = fs.readFileSync(
  path.resolve(__dirname, '../../../clickhouse/init/01-init.sql'),
  'utf-8'
)

function getTableColumns(sql: string, tableName: string): string[] {
  const regex = new RegExp(
    `CREATE TABLE IF NOT EXISTS ${tableName}\\s*\\((.+?)\\)\\s*ENGINE`,
    's'
  )
  const match = sql.match(regex)
  if (!match) return []
  return match[1]
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('--'))
    .map((line) => line.split(/\s+/)[0])
    .filter(Boolean)
}

describe('ClickHouse init schema', () => {
  test('datasets_metadata has required columns', () => {
    const columns = getTableColumns(INIT_SQL, 'datasets_metadata')
    expect(columns).toContain('dataset_id')
    expect(columns).toContain('dataset_name')
    expect(columns).toContain('database_name')
    expect(columns).toContain('database_type')
    expect(columns).toContain('description')
    expect(columns).toContain('created_by')
    expect(columns).toContain('created_at')
  })

  test('dataset_tables has required columns', () => {
    const columns = getTableColumns(INIT_SQL, 'dataset_tables')
    expect(columns).toContain('dataset_id')
    expect(columns).toContain('table_id')
    expect(columns).toContain('table_name')
    expect(columns).toContain('clickhouse_table_name')
    expect(columns).toContain('row_count')
  })

  test('dataset_columns has required columns', () => {
    const columns = getTableColumns(INIT_SQL, 'dataset_columns')
    expect(columns).toContain('dataset_id')
    expect(columns).toContain('table_id')
    expect(columns).toContain('column_name')
    expect(columns).toContain('column_type')
  })

  test('table_relationships has required columns', () => {
    const columns = getTableColumns(INIT_SQL, 'table_relationships')
    expect(columns).toContain('dataset_id')
    expect(columns).toContain('table_id')
    expect(columns).toContain('foreign_key')
    expect(columns).toContain('referenced_table')
    expect(columns).toContain('referenced_column')
  })
})

/**
 * Filter types used by both client and server for cross-table filtering.
 */

export interface Filter {
  /** Column name (leaf node) */
  column?: string
  /** Filter operator */
  operator?: 'eq' | 'in' | 'gt' | 'lt' | 'gte' | 'lte' | 'between'
  /** Filter value */
  value?: any

  /** Logical AND of sub-filters */
  and?: Filter[]
  /** Logical OR of sub-filters */
  or?: Filter[]
  /** Logical NOT of a sub-filter */
  not?: Filter

  /** Table this filter applies to (cross-table metadata) */
  tableName?: string
  /**
   * Client-side metadata describing which count context (rows vs parent table) produced the filter.
   * The backend ignores this field and only relies on {@link tableName} to derive join paths.
   */
  countByKey?: string
}

export interface TableRelationship {
  foreign_key: string
  referenced_table: string
  referenced_column: string
  type?: string
}

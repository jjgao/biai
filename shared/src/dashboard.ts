/**
 * Dashboard types shared between client and server.
 */

export interface DashboardChart {
  tableName: string
  columnName: string
  compareColumn?: string
  countByTarget?: string | null
  addedAt: string
}

import clickhouseClient from '../config/clickhouse.js'

// Re-export from shared so existing imports continue to work
export type { DashboardChart } from 'shared'
import type { DashboardChart } from 'shared'

export interface Dashboard {
  dashboard_id: string
  dataset_id: string
  dashboard_name: string
  charts: DashboardChart[]
  is_most_recent: boolean
  created_at: string | Date
  updated_at: string | Date
}

export class DashboardService {
  // Ensure the dashboards table exists
  async initializeTable(): Promise<void> {
    await clickhouseClient.command({
      query: `
        CREATE TABLE IF NOT EXISTS biai.dataset_dashboards (
          dashboard_id String,
          dataset_id String,
          dashboard_name String,
          charts String,
          is_most_recent UInt8,
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (dataset_id, dashboard_id)
      `
    })
  }

  // Get all dashboards for a dataset
  async listDashboards(datasetId: string): Promise<Dashboard[]> {
    const result = await clickhouseClient.query({
      query: `
        SELECT *
        FROM biai.dataset_dashboards
        WHERE dataset_id = {datasetId:String}
        ORDER BY is_most_recent DESC, updated_at DESC
      `,
      query_params: { datasetId },
      format: 'JSONEachRow'
    })

    const dashboards = await result.json<any>()

    // Older dashboard payloads (pre-MVP3) may not include countByTarget; default to null when absent
    return dashboards.map((d: any) => ({
      dashboard_id: d.dashboard_id,
      dataset_id: d.dataset_id,
      dashboard_name: d.dashboard_name,
      charts: JSON.parse(d.charts).map((chart: any) => ({
        tableName: chart.tableName,
        columnName: chart.columnName,
        countByTarget: chart.countByTarget ?? null,
        addedAt: chart.addedAt
      })),
      is_most_recent: Boolean(d.is_most_recent),
      created_at: d.created_at,
      updated_at: d.updated_at
    }))
  }

  // Get a specific dashboard
  async getDashboard(datasetId: string, dashboardId: string): Promise<Dashboard | null> {
    const result = await clickhouseClient.query({
      query: `
        SELECT *
        FROM biai.dataset_dashboards
        WHERE dataset_id = {datasetId:String}
          AND dashboard_id = {dashboardId:String}
        LIMIT 1
      `,
      query_params: { datasetId, dashboardId },
      format: 'JSONEachRow'
    })

    const dashboards = await result.json<any>()
    if (dashboards.length === 0) return null

    const d = dashboards[0]
    return {
      dashboard_id: d.dashboard_id,
      dataset_id: d.dataset_id,
      dashboard_name: d.dashboard_name,
      charts: JSON.parse(d.charts).map((chart: any) => ({
        tableName: chart.tableName,
        columnName: chart.columnName,
        countByTarget: chart.countByTarget ?? null,
        addedAt: chart.addedAt
      })),
      is_most_recent: Boolean(d.is_most_recent),
      created_at: d.created_at,
      updated_at: d.updated_at
    }
  }

  // Save or update a dashboard
  async saveDashboard(
    datasetId: string,
    dashboardId: string,
    dashboardName: string,
    charts: DashboardChart[],
    isMostRecent: boolean = false
  ): Promise<Dashboard> {
    // Check if dashboard exists
    const existing = await this.getDashboard(datasetId, dashboardId)

    if (existing) {
      // Update existing dashboard
      await clickhouseClient.command({
        query: `
          ALTER TABLE biai.dataset_dashboards
          UPDATE
            dashboard_name = {dashboardName:String},
            charts = {charts:String},
            updated_at = now()
          WHERE dataset_id = {datasetId:String}
            AND dashboard_id = {dashboardId:String}
        `,
        query_params: {
          datasetId,
          dashboardId,
          dashboardName,
          charts: JSON.stringify(charts)
        }
      })
    } else {
      // Insert new dashboard
      await clickhouseClient.insert({
        table: 'biai.dataset_dashboards',
        values: [{
          dashboard_id: dashboardId,
          dataset_id: datasetId,
          dashboard_name: dashboardName,
          charts: JSON.stringify(charts),
          is_most_recent: isMostRecent ? 1 : 0,
          created_at: Math.floor(Date.now() / 1000),
          updated_at: Math.floor(Date.now() / 1000)
        }],
        format: 'JSONEachRow'
      })
    }

    const dashboard = await this.getDashboard(datasetId, dashboardId)
    if (!dashboard) {
      throw new Error('Failed to save dashboard')
    }

    return dashboard
  }

  // Delete a dashboard
  async deleteDashboard(datasetId: string, dashboardId: string): Promise<void> {
    await clickhouseClient.command({
      query: `
        DELETE FROM biai.dataset_dashboards
        WHERE dataset_id = {datasetId:String}
          AND dashboard_id = {dashboardId:String}
      `,
      query_params: { datasetId, dashboardId }
    })
  }

  // Delete all dashboards for a dataset
  async deleteAllDashboards(datasetId: string): Promise<void> {
    await clickhouseClient.command({
      query: `
        DELETE FROM biai.dataset_dashboards
        WHERE dataset_id = {datasetId:String}
      `,
      query_params: { datasetId }
    })
  }
}

const dashboardService = new DashboardService()
export default dashboardService

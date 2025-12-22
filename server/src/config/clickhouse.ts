import { createClient, ClickHouseClient, ClickHouseClientConfigOptions } from '@clickhouse/client'
import dotenv from 'dotenv'

dotenv.config()

const clickhouseClient = createClient({
  url: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'biai',
})

export interface ClickHouseConnectionSettings {
  host: string
  port?: number
  protocol?: 'http' | 'https'
  database?: string
  username?: string
  password?: string
}

export const createClickHouseClient = (settings: ClickHouseConnectionSettings): ClickHouseClient => {
  const protocol = settings.protocol || 'http'
  const port = settings.port ?? (protocol === 'https' ? 8443 : 8123)
  const url = `${protocol}://${settings.host}:${port}`

  const config: ClickHouseClientConfigOptions = {
    url,
    database: settings.database || 'default',
    request_timeout: 5000, // 5 second timeout for remote connections
  }

  if (settings.username) {
    config.username = settings.username
  }
  if (settings.password) {
    config.password = settings.password
  }

  return createClient(config)
}

export const testConnection = async () => {
  try {
    const result = await clickhouseClient.query({
      query: 'SELECT 1',
      format: 'JSONEachRow',
    })
    const data = await result.json()
    console.log('ClickHouse connection successful:', data)
    return true
  } catch (error) {
    console.error('ClickHouse connection failed:', error)
    return false
  }
}

export default clickhouseClient

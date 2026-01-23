import clickhouseClient from '../config/clickhouse.js'

/**
 * Get the list of tables in the current ClickHouse database.
 * This is a safe, predefined query with no user input.
 */
export const getTablesList = async () => {
  try {
    const result = await clickhouseClient.query({
      query: 'SHOW TABLES',
      format: 'JSONEachRow',
    })
    return await result.json()
  } catch (error) {
    console.error('Query execution error:', error)
    throw error
  }
}

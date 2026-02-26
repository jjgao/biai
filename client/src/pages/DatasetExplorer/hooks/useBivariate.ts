import { useState, useEffect, useRef } from 'react'

const BIVARIATE_STORAGE_PREFIX = 'bivariate_'

interface UseBivariateArgs {
  identifier: string | undefined
}

/**
 * Manages bivariate (2-variable) chart selections.
 *
 * `bivariateSelections` maps `tableName.columnName` → second column name.
 * When a selection exists, the univariate chart is replaced by a StackedBarChart.
 * Selections are persisted in localStorage.
 */
export function useBivariate({ identifier }: UseBivariateArgs) {
  const [bivariateSelections, setBivariateSelections] = useState<Record<string, string>>({})
  const initialized = useRef(false)

  // Reset on identifier change
  useEffect(() => {
    initialized.current = false
    setBivariateSelections({})
  }, [identifier])

  // Load from localStorage
  useEffect(() => {
    if (initialized.current || !identifier) return
    try {
      const stored = localStorage.getItem(`${BIVARIATE_STORAGE_PREFIX}${identifier}`)
      if (stored) {
        setBivariateSelections(JSON.parse(stored))
      }
    } catch (error) {
      console.error('Failed to load bivariate selections:', error)
    }
    initialized.current = true
  }, [identifier])

  // Persist to localStorage
  useEffect(() => {
    if (!initialized.current || !identifier) return
    try {
      const key = `${BIVARIATE_STORAGE_PREFIX}${identifier}`
      if (Object.keys(bivariateSelections).length === 0) {
        localStorage.removeItem(key)
      } else {
        localStorage.setItem(key, JSON.stringify(bivariateSelections))
      }
    } catch (error) {
      console.error('Failed to persist bivariate selections:', error)
    }
  }, [bivariateSelections, identifier])

  const bivariateKey = (tableName: string, columnName: string) =>
    `${tableName}.${columnName}`

  const getBivariateSelection = (tableName: string, columnName: string): string | undefined =>
    bivariateSelections[bivariateKey(tableName, columnName)]

  const setBivariateSelection = (tableName: string, columnName: string, compareColumn?: string) => {
    setBivariateSelections(prev => {
      const key = bivariateKey(tableName, columnName)
      if (!compareColumn) {
        if (!(key in prev)) return prev
        const { [key]: _removed, ...rest } = prev
        return rest
      }
      if (prev[key] === compareColumn) return prev
      return { ...prev, [key]: compareColumn }
    })
  }

  return {
    bivariateSelections,
    getBivariateSelection,
    setBivariateSelection,
  }
}

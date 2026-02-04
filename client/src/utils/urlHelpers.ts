/**
 * Utilities for encoding/decoding application state to URL hashes or local storage
 */

/**
 * Encode a serializable state object to a base64 string safe for URLs
 */
export const encodeState = (state: any): string => {
  try {
      const json = JSON.stringify(state)
      return btoa(encodeURIComponent(json))
  } catch (error) {
      console.error('Failed to encode state:', error)
      return ''
  }
}

/**
 * Decode a base64 string from URL back to a state object
 */
export const decodeState = <T>(encoded: string): T | null => {
  try {
      const json = decodeURIComponent(atob(encoded))
      return JSON.parse(json)
  } catch (error) {
      console.error('Failed to decode state:', error)
      return null
  }
}

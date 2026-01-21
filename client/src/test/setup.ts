import '@testing-library/jest-dom/vitest'
import { expect, afterEach, beforeAll } from 'vitest'
import { cleanup } from '@testing-library/react'
import * as matchers from '@testing-library/jest-dom/matchers'

expect.extend(matchers)

// Mock localStorage for jsdom environment (Vite 7.x compatibility)
beforeAll(() => {
  const localStorageMock = (() => {
    let store: Record<string, string> = {}
    return {
      getItem: (key: string) => store[key] || null,
      setItem: (key: string, value: string) => { store[key] = value },
      removeItem: (key: string) => { delete store[key] },
      clear: () => { store = {} },
      get length() { return Object.keys(store).length },
      key: (index: number) => Object.keys(store)[index] || null
    }
  })()

  Object.defineProperty(window, 'localStorage', {
    value: localStorageMock,
    writable: true
  })
})

afterEach(() => {
  cleanup()
  // Clear localStorage between tests
  localStorage.clear()
})

import { useState, useEffect, useCallback } from 'react'

const KEY = 'fg-theme'

function readInitialTheme(): 'dark' | 'light' {
  if (typeof window === 'undefined') return 'dark'
  const stored = localStorage.getItem(KEY)
  if (stored === 'dark' || stored === 'light') return stored
  // Fallback to system preference, default dark
  return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
}

export function useTheme() {
  const [isDark, setIsDark] = useState<boolean>(() => readInitialTheme() === 'dark')

  useEffect(() => {
    const root = document.documentElement
    const theme = isDark ? 'dark' : 'light'
    // Update the attribute (used by the standalone homepage's CSS)
    root.dataset.theme = theme
    // Update the class (used by the React app's index.css tokens)
    if (isDark) root.classList.add('dark')
    else        root.classList.remove('dark')
    localStorage.setItem(KEY, theme)
  }, [isDark])

  const toggle = useCallback(() => setIsDark(prev => !prev), [])

  return { isDark, toggle }
}

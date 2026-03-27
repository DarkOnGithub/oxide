import { useEffect, useLayoutEffect, useState, useRef } from 'react'
import { emit } from '@tauri-apps/api/event'
import { ThemeProviderContext } from '@/lib/theme-context'
import { usePreferences } from '@/services/preferences'
import {
  isTheme,
  resolveAppliedTheme,
  resolveThemeMode,
  type ResolvedThemeMode,
  type Theme,
} from '@/lib/theme'

interface ThemeProviderProps {
  children: React.ReactNode
  defaultTheme?: Theme
  storageKey?: string
}

export function ThemeProvider({
  children,
  defaultTheme = 'system',
  storageKey = 'ui-theme',
  ...props
}: ThemeProviderProps) {
  const [theme, setTheme] = useState<Theme>(
    () => {
      const storedTheme = localStorage.getItem(storageKey)
      return isTheme(storedTheme) ? storedTheme : defaultTheme
    }
  )
  const [resolvedTheme, setResolvedTheme] =
    useState<ResolvedThemeMode>('light')

  // Load theme from persistent preferences
  const { data: preferences } = usePreferences()
  const hasSyncedPreferences = useRef(false)

  // Sync theme with preferences when they load
  // This is a legitimate case of syncing with external async state (persistent preferences)
  // The ref ensures this only happens once when preferences first load
  useLayoutEffect(() => {
    if (
      preferences?.theme &&
      isTheme(preferences.theme) &&
      !hasSyncedPreferences.current
    ) {
      hasSyncedPreferences.current = true
      // eslint-disable-next-line react-hooks/set-state-in-effect -- Syncing with external async preferences on initial load
      setTheme(preferences.theme)
    }
  }, [preferences?.theme])

  useEffect(() => {
    const root = window.document.documentElement
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')

    const applyTheme = (prefersDark: boolean) => {
      const appliedTheme = resolveAppliedTheme(theme, prefersDark)
      const nextResolvedTheme = resolveThemeMode(theme, prefersDark)

      root.classList.remove('light', 'dark')
      root.classList.add(nextResolvedTheme)
      root.dataset.theme = appliedTheme
      root.style.colorScheme = nextResolvedTheme
      setResolvedTheme(nextResolvedTheme)
    }

    if (theme === 'system') {
      applyTheme(mediaQuery.matches)

      const handleChange = (e: MediaQueryListEvent) => applyTheme(e.matches)
      mediaQuery.addEventListener('change', handleChange)
      return () => mediaQuery.removeEventListener('change', handleChange)
    }

    applyTheme(false)
  }, [theme])

  const value = {
    theme,
    resolvedTheme,
    setTheme: (newTheme: Theme) => {
      localStorage.setItem(storageKey, newTheme)
      setTheme(newTheme)
      // Notify other windows (e.g., quick pane) of theme change
      emit('theme-changed', { theme: newTheme })
    },
  }

  return (
    <ThemeProviderContext.Provider {...props} value={value}>
      {children}
    </ThemeProviderContext.Provider>
  )
}

import { createContext } from 'react'
import type { ResolvedThemeMode, Theme } from './theme'

export interface ThemeProviderState {
  theme: Theme
  resolvedTheme: ResolvedThemeMode
  setTheme: (theme: Theme) => void
}

const initialState: ThemeProviderState = {
  theme: 'system',
  resolvedTheme: 'light',
  setTheme: () => null,
}

export const ThemeProviderContext =
  createContext<ThemeProviderState>(initialState)

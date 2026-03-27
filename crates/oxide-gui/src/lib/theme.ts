export type Theme =
  | 'light'
  | 'dark'
  | 'latte'
  | 'frappe'
  | 'macchiato'
  | 'mocha'
  | 'system'

export type AppliedTheme = Exclude<Theme, 'system'>
export type ResolvedThemeMode = 'light' | 'dark'

export const THEME_OPTIONS: { value: Theme; label: string }[] = [
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
  { value: 'latte', label: 'Catppuccin Latte' },
  { value: 'frappe', label: 'Catppuccin Frappe' },
  { value: 'macchiato', label: 'Catppuccin Macchiato' },
  { value: 'mocha', label: 'Catppuccin Mocha' },
  { value: 'system', label: 'System' },
]

const darkThemes = new Set<AppliedTheme>(['dark', 'frappe', 'macchiato', 'mocha'])

export function isTheme(value: string | null | undefined): value is Theme {
  return THEME_OPTIONS.some(option => option.value === value)
}

export function resolveAppliedTheme(
  theme: Theme,
  prefersDark: boolean
): AppliedTheme {
  if (theme === 'system') {
    return prefersDark ? 'dark' : 'light'
  }

  return theme
}

export function isDarkTheme(theme: AppliedTheme): boolean {
  return darkThemes.has(theme)
}

export function resolveThemeMode(
  theme: Theme,
  prefersDark: boolean
): ResolvedThemeMode {
  return isDarkTheme(resolveAppliedTheme(theme, prefersDark)) ? 'dark' : 'light'
}

export function resolveIconTheme(
  theme: Theme,
  resolvedTheme: ResolvedThemeMode
): 'latte' | 'frappe' | 'macchiato' | 'mocha' {
  switch (theme) {
    case 'latte':
      return 'latte'
    case 'frappe':
      return 'frappe'
    case 'macchiato':
      return 'macchiato'
    case 'mocha':
      return 'mocha'
    case 'light':
      return 'latte'
    case 'dark':
      return 'mocha'
    case 'system':
      return resolvedTheme === 'dark' ? 'mocha' : 'latte'
  }
}

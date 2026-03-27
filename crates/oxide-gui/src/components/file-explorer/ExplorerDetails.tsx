import { CalendarDays, FileStack, Palette, Sparkles } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { FileIcon } from './FileIcon'
import type { FileExplorerState } from './use-file-explorer'
import { useTheme } from '@/hooks/use-theme'
import { usePreferences, useSavePreferences } from '@/services/preferences'
import { THEME_OPTIONS, type Theme } from '@/lib/theme'

interface ExplorerDetailsProps {
  explorer: FileExplorerState
}

function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes < 0) {
    return '—'
  }

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = bytes
  let unitIndex = 0

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }

  return `${size.toFixed(size >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function formatDate(date: Date | null) {
  if (!date) {
    return '—'
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date)
}

export function ExplorerDetails({ explorer }: ExplorerDetailsProps) {
  const { t } = useTranslation()
  const { theme, setTheme, resolvedTheme } = useTheme()
  const { data: preferences } = usePreferences()
  const savePreferences = useSavePreferences()

  const handleThemeChange = (value: Theme) => {
    setTheme(value)

    if (preferences) {
      savePreferences.mutate({ ...preferences, theme: value })
    }
  }

  const selectedName = explorer.selectedEntry?.name ?? explorer.currentName ?? 'No selection'
  const selectedPath = explorer.selectedEntry?.path ?? explorer.currentPath ?? '—'
  const selectedIsDirectory = explorer.selectedEntry?.isDirectory ?? true

  const themeLabel = (value: Theme) => {
    switch (value) {
      case 'light':
        return t('preferences.appearance.theme.light')
      case 'dark':
        return t('preferences.appearance.theme.dark')
      case 'system':
        return t('preferences.appearance.theme.system')
      case 'latte':
        return t('preferences.appearance.theme.latte')
      case 'frappe':
        return t('preferences.appearance.theme.frappe')
      case 'macchiato':
        return t('preferences.appearance.theme.macchiato')
      case 'mocha':
        return t('preferences.appearance.theme.mocha')
    }
  }

  return (
    <div className="flex h-full min-h-0 flex-col px-4 py-4">
      <div className="space-y-4">
        <div className="rounded-2xl border border-border/70 bg-surface-elevated/80 p-4 shadow-sm">
          <div className="mb-3 flex items-center gap-2">
            <Palette className="size-4 text-primary" />
            <h3 className="text-sm font-semibold text-foreground">Appearance</h3>
          </div>

          <Select value={theme} onValueChange={value => handleThemeChange(value as Theme)}>
            <SelectTrigger className="w-full rounded-xl bg-background/80">
              <SelectValue placeholder={t('preferences.appearance.selectTheme')} />
            </SelectTrigger>
            <SelectContent>
              {THEME_OPTIONS.map(option => (
                <SelectItem key={option.value} value={option.value}>
                  {themeLabel(option.value)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <p className="mt-3 text-xs leading-5 text-muted-foreground">
            Active UI mode: <span className="font-medium text-foreground">{resolvedTheme}</span>
          </p>
        </div>

        <div className="rounded-2xl border border-border/70 bg-surface-elevated/80 p-4 shadow-sm">
          <div className="flex items-start gap-3">
            <div className="flex size-11 items-center justify-center rounded-2xl bg-background/80 ring-1 ring-border/60">
              <FileIcon
                name={selectedName}
                isDirectory={selectedIsDirectory}
                open={selectedIsDirectory}
                className="size-5"
              />
            </div>

            <div className="min-w-0 flex-1">
              <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
                Inspector
              </p>
              <h3 className="truncate text-sm font-semibold text-foreground">
                {selectedName}
              </h3>
              <p className="mt-1 line-clamp-2 text-xs text-muted-foreground">
                {selectedPath}
              </p>
            </div>
          </div>

          <div className="mt-4 space-y-3 text-sm">
            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Type</span>
              <span className="font-medium text-foreground">
                {explorer.selectedEntry
                  ? explorer.selectedEntry.isDirectory
                    ? 'Folder'
                    : explorer.selectedEntry.extension
                      ? `${explorer.selectedEntry.extension.toUpperCase()} file`
                      : 'File'
                  : explorer.currentPath
                    ? 'Current folder'
                    : '—'}
              </span>
            </div>

            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Items</span>
              <span className="font-medium text-foreground">
                {explorer.selectedEntry?.isDirectory || !explorer.selectedEntry
                  ? explorer.entries.length
                  : '—'}
              </span>
            </div>

            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Size</span>
              <span className="font-medium text-foreground">
                {formatBytes(explorer.selectedEntryInfo?.size ?? 0)}
              </span>
            </div>

            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Modified</span>
              <span className="text-right font-medium text-foreground">
                {formatDate(explorer.selectedEntryInfo?.mtime ?? null)}
              </span>
            </div>
          </div>
        </div>
      </div>

      <div className="mt-4 grid gap-3">
        <div className="rounded-2xl border border-border/70 bg-surface-elevated/80 p-4 shadow-sm">
          <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
            <FileStack className="size-4 text-primary" />
            Directory snapshot
          </div>

          <div className="mt-3 space-y-2 text-sm">
            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Folders</span>
              <span className="font-medium text-foreground">
                {explorer.directoryEntries.length}
              </span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Files</span>
              <span className="font-medium text-foreground">
                {explorer.fileEntries.length}
              </span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Selected path</span>
              <span className="max-w-[8rem] truncate font-medium text-foreground">
                {explorer.selectedEntry?.name ?? 'Current folder'}
              </span>
            </div>
          </div>
        </div>

        <div className="rounded-2xl border border-border/70 bg-gradient-to-br from-primary/8 via-surface-elevated to-surface p-4 shadow-sm">
          <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
            <Sparkles className="size-4 text-primary" />
            Next step
          </div>
          <p className="mt-2 text-sm leading-6 text-muted-foreground">
            The explorer foundation is ready for archive browsing, extraction, and drag-and-drop flows.
          </p>
          <div className="mt-3 flex items-center gap-2 text-xs text-muted-foreground">
            <CalendarDays className="size-3.5" />
            Minimal version ready for archiver integration.
          </div>
        </div>
      </div>
    </div>
  )
}

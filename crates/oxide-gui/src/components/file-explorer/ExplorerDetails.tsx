import { Settings, Terminal } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import { Button } from '@/components/ui/button'
import { FileIcon } from './FileIcon'
import type { FileExplorerState } from './use-file-explorer'
import { useUIStore } from '@/store/ui-store'
import { openTerminalInFolder } from '@/lib/terminal'
import { toast } from 'sonner'

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

function formatPermissions(
  mode: number | null,
  readonly: boolean | null | undefined
) {
  if (mode !== null) {
    const bits = [
      0o400, 0o200, 0o100, 0o040, 0o020, 0o010, 0o004, 0o002, 0o001,
    ]
    const labels = ['r', 'w', 'x', 'r', 'w', 'x', 'r', 'w', 'x']

    return bits.map((bit, index) => (mode & bit ? labels[index] : '-')).join('')
  }

  if (readonly === true) {
    return 'Read-only'
  }

  if (readonly === false) {
    return 'Writable'
  }

  return '—'
}

export function ExplorerDetails({ explorer }: ExplorerDetailsProps) {
  const { t } = useTranslation()
  const setPreferencesOpen = useUIStore(state => state.setPreferencesOpen)
  const terminalPath = explorer.terminalPath
  const info = explorer.selectedEntryInfo
  const isCurrentFolder = !explorer.selectedEntry && Boolean(explorer.currentPath)

  const selectedName = explorer.selectedEntry?.name ?? explorer.currentName ?? 'No selection'
  const selectedPath = explorer.selectedDisplayPath
  const selectedIsDirectory = explorer.selectedEntry?.isDirectory ?? true
  const modifiedAt = info?.modifiedAt ?? explorer.selectedEntry?.modifiedAt ?? null
  const createdAt = info?.createdAt ?? null
  const accessedAt = info?.accessedAt ?? null
  const selectedSize = explorer.selectedEntry
    ? explorer.selectedEntry.size
    : (info?.size ?? 0)

  const handleOpenTerminal = async () => {
    if (!terminalPath) {
      return
    }

    try {
      await openTerminalInFolder(terminalPath)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to open terminal'
      toast.error('Unable to open terminal', { description: message })
    }
  }

  return (
    <div className="flex h-full min-h-0 flex-col bg-surface/30">
      <div className="flex items-center justify-between border-b border-border/40 px-4 py-4">
        <h3 className="text-lg font-medium text-foreground">Properties</h3>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="icon"
            className="size-8 rounded-lg"
            onClick={() => setPreferencesOpen(true)}
            title={t('titlebar.settings')}
          >
            <Settings className="size-4" />
          </Button>

          <Button
            variant="outline"
            size="icon"
            className="size-8 rounded-lg"
            onClick={() => void handleOpenTerminal()}
            disabled={!terminalPath}
            title="Open terminal here"
          >
            <Terminal className="size-4" />
          </Button>
        </div>
      </div>

      <div className="flex min-h-0 flex-1 flex-col gap-4 px-4 py-4">
        <div className="border-b border-border/30 pb-4 text-center">
          <div className="mx-auto flex size-14 items-center justify-center rounded-2xl bg-background/80 ring-1 ring-border/60">
            <FileIcon
              name={selectedName}
              isDirectory={selectedIsDirectory}
              open={selectedIsDirectory}
              className="size-6"
            />
          </div>
          <h4 className="mt-4 truncate text-xl font-medium text-foreground">
            {selectedName}
          </h4>
          <p className="mt-1 text-sm text-muted-foreground">
            {selectedIsDirectory ? 'Folder' : 'File'}
          </p>
        </div>

        <div className="space-y-5 text-sm text-foreground">
          <section className="space-y-2">
            <h5 className="text-base font-medium text-foreground">General</h5>
            <div className="space-y-2 border-t border-border/30 pt-3">
              <Row label="Type" value={selectedIsDirectory ? 'Folder' : 'File'} />
              <Row label="Location" value={selectedPath} />
              <Row
                label="Size"
                value={explorer.selectedEntry ? formatBytes(selectedSize) : info ? formatBytes(selectedSize) : '—'}
              />
              {isCurrentFolder && (
                <Row
                  label="Contents"
                  value={`${explorer.entries.length} items, ${explorer.directoryEntries.length} folders`}
                />
              )}
              <Row
                label="Read only"
                value={
                  info?.readonly === null || info?.readonly === undefined
                    ? '—'
                    : info.readonly
                      ? 'Yes'
                      : 'No'
                }
              />
              {info?.target && <Row label="Target" value={info.target} />}
            </div>
          </section>

          <section className="space-y-2">
            <h5 className="text-base font-medium text-foreground">Dates</h5>
            <div className="space-y-2 border-t border-border/30 pt-3">
              <Row label="Created" value={formatDate(createdAt)} />
              <Row label="Modified" value={formatDate(modifiedAt)} />
              <Row label="Accessed" value={formatDate(accessedAt)} />
            </div>
          </section>

          <section className="space-y-2">
            <h5 className="text-base font-medium text-foreground">Permissions</h5>
            <div className="space-y-2 border-t border-border/30 pt-3">
              <Row
                label="Access rights"
                value={formatPermissions(info?.mode ?? null, info?.readonly)}
              />
              <Row label="Symlink" value={explorer.selectedEntry?.isSymlink ? 'Yes' : 'No'} />
              <Row label="Owner" value={info?.uid !== null && info?.uid !== undefined ? String(info.uid) : '—'} />
            </div>
          </section>
        </div>
      </div>
    </div>
  )
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-4 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="max-w-[11rem] text-right text-foreground">{value}</span>
    </div>
  )
}

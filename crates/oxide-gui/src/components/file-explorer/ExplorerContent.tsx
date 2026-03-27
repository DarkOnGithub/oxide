import {
  ChevronRight,
  FolderOpen,
  LoaderCircle,
  RefreshCw,
  Undo2,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { FileIcon } from './FileIcon'
import type { FileExplorerState } from './use-file-explorer'

interface ExplorerContentProps {
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
  }).format(date)
}

export function ExplorerContent({ explorer }: ExplorerContentProps) {
  return (
    <div className="flex h-full min-h-0 flex-col bg-surface/20 px-3 py-3">
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-2xl border border-border/40 bg-background/70 shadow-sm backdrop-blur-xl">
        <div className="border-b border-border/40 bg-background/70 px-3 py-2.5">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="icon"
                className="size-8 rounded-lg"
                onClick={explorer.goToParent}
                disabled={!explorer.canGoUp}
                title="Up"
              >
                <Undo2 className="size-4" />
              </Button>

              <Button
                variant="ghost"
                size="icon"
                className="size-8 rounded-lg"
                onClick={explorer.refresh}
                disabled={!explorer.currentPath || explorer.isRefreshing}
                title="Refresh"
              >
                <RefreshCw
                  className={cn('size-4', explorer.isRefreshing && 'animate-spin')}
                />
              </Button>

              <div className="mx-1 h-5 w-px bg-border/60" />
            </div>

            <Button
              className="h-8 rounded-lg px-3"
              onClick={explorer.pickRootFolder}
              disabled={explorer.isPickingFolder}
            >
              <FolderOpen className="size-4" />
              Open folder
            </Button>
          </div>

          <div className="mt-3 flex min-w-0 items-center gap-2 rounded-xl border border-border/40 bg-muted/40 px-3 py-2">
            <FolderOpen className="size-4 shrink-0 text-muted-foreground" />

            {explorer.breadcrumbs.length > 0 ? (
              <div className="flex min-w-0 items-center gap-1.5 overflow-hidden text-sm">
                {explorer.breadcrumbs.map((crumb, index) => (
                  <div key={crumb.path} className="flex min-w-0 items-center gap-1.5">
                    <button
                      type="button"
                      className="truncate rounded-md px-1.5 py-0.5 text-left text-foreground transition-colors hover:bg-background/70"
                      onClick={() => explorer.openPath(crumb.path)}
                    >
                      {crumb.label}
                    </button>

                    {index < explorer.breadcrumbs.length - 1 && (
                      <ChevronRight className="size-3.5 shrink-0 text-muted-foreground" />
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <span className="truncate text-sm text-foreground">
                {explorer.currentPath ?? 'Select a folder'}
              </span>
            )}
          </div>
        </div>

        {!explorer.currentPath ? (
          <div className="flex min-h-0 flex-1 items-center justify-center p-8 text-center">
            <div className="max-w-sm">
              <FolderOpen className="mx-auto size-10 text-muted-foreground/70" />
              <h3 className="mt-4 text-lg font-semibold text-foreground">
                Open a folder to begin
              </h3>
              <p className="mt-2 text-sm text-muted-foreground">
                The explorer will show a clean list view once a folder is selected.
              </p>
              <Button className="mt-4 rounded-lg" onClick={explorer.pickRootFolder}>
                Open folder
              </Button>
            </div>
          </div>
        ) : (
          <>
            {explorer.error && (
              <div className="mx-3 mt-3 rounded-xl border border-destructive/20 bg-destructive/8 px-3 py-2 text-sm text-destructive">
                {explorer.error}
              </div>
            )}

            <div className="border-b border-border/40 px-4 py-3">
              <div className="grid grid-cols-[minmax(0,1fr)_92px_120px_140px] gap-4 text-xs font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                <span>Name</span>
                <span>Size</span>
                <span>Type</span>
                <span>Modified</span>
              </div>
            </div>

            <ScrollArea className="min-h-0 flex-1">
              <div className="px-2 py-1">
                {explorer.isLoading && explorer.entries.length === 0 ? (
                  <div className="flex min-h-[360px] items-center justify-center gap-3 text-sm text-muted-foreground">
                    <LoaderCircle className="size-4 animate-spin" />
                    Loading folder contents...
                  </div>
                ) : explorer.entries.length === 0 ? (
                  <div className="flex min-h-[360px] items-center justify-center text-sm text-muted-foreground">
                    This folder is empty.
                  </div>
                ) : (
                  explorer.entries.map(entry => {
                    const isSelected = explorer.selectedEntryPath === entry.path

                    return (
                      <button
                        key={entry.path}
                        type="button"
                        onClick={() => explorer.setSelectedEntryPath(entry.path)}
                        onDoubleClick={() => explorer.openEntry(entry)}
                        className={cn(
                          'grid w-full grid-cols-[minmax(0,1fr)_92px_120px_140px] items-center gap-4 border-b border-border/30 px-4 py-3 text-left text-sm transition-colors',
                          isSelected
                            ? 'bg-selection/70 text-selection-foreground'
                            : 'bg-transparent hover:bg-surface-elevated/70'
                        )}
                      >
                        <div className="flex min-w-0 items-center gap-3">
                          <FileIcon
                            name={entry.name}
                            isDirectory={entry.isDirectory}
                            open={isSelected && entry.isDirectory}
                            className="size-5 shrink-0"
                          />
                          <div className="min-w-0">
                            <p className="truncate font-medium">{entry.name}</p>
                          </div>
                        </div>

                        <span className="truncate text-muted-foreground">
                          {entry.isDirectory ? '—' : formatBytes(entry.size)}
                        </span>

                        <span className="truncate text-muted-foreground">
                          {entry.isDirectory
                            ? 'Folder'
                            : entry.extension
                              ? `${entry.extension.toUpperCase()} File`
                              : 'File'}
                        </span>

                        <span className="truncate text-muted-foreground">
                          {formatDate(entry.modifiedAt)}
                        </span>
                      </button>
                    )
                  })
                )}
              </div>
            </ScrollArea>

            <div className="flex items-center justify-between gap-2 border-t border-border/40 px-4 py-2 text-xs text-muted-foreground">
              <span>
                {explorer.entries.length} items · {explorer.directoryEntries.length} folders ·{' '}
                {explorer.fileEntries.length} files
              </span>
              <span>Select an item to inspect it</span>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

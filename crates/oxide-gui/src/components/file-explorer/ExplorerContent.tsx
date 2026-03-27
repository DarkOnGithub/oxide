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

export function ExplorerContent({ explorer }: ExplorerContentProps) {
  return (
    <div className="flex h-full min-h-0 flex-col px-5 py-5">
      <div className="flex min-h-0 flex-1 flex-col rounded-[28px] border border-border/70 bg-surface/85 shadow-[0_18px_70px_-48px_rgba(15,23,42,0.6)] backdrop-blur-xl">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border/60 px-5 py-4">
          <div className="min-w-0 flex-1">
            <div className="flex min-w-0 flex-wrap items-center gap-1.5 text-sm">
              {explorer.breadcrumbs.length > 0 ? (
                explorer.breadcrumbs.map((crumb, index) => (
                  <div key={crumb.path} className="flex items-center gap-1.5">
                    <button
                      type="button"
                      className="cursor-pointer truncate rounded-lg px-2 py-1 text-sm font-medium text-foreground/80 transition-colors hover:bg-surface-elevated hover:text-foreground"
                      onClick={() => explorer.openPath(crumb.path)}
                    >
                      {crumb.label}
                    </button>

                    {index < explorer.breadcrumbs.length - 1 && (
                      <ChevronRight className="size-4 text-muted-foreground" />
                    )}
                  </div>
                ))
              ) : (
                <span className="text-sm text-muted-foreground">
                  No folder selected
                </span>
              )}
            </div>

            <p className="mt-1 truncate text-xs text-muted-foreground">
              {explorer.currentPath ?? 'Select a folder on disk to start browsing.'}
            </p>
          </div>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="cursor-pointer rounded-xl"
              onClick={explorer.goToParent}
              disabled={!explorer.canGoUp}
            >
              <Undo2 className="size-4" />
              Up
            </Button>

            <Button
              variant="outline"
              size="sm"
              className="cursor-pointer rounded-xl"
              onClick={explorer.refresh}
              disabled={!explorer.currentPath || explorer.isRefreshing}
            >
              <RefreshCw
                className={cn('size-4', explorer.isRefreshing && 'animate-spin')}
              />
              Refresh
            </Button>

            <Button
              size="sm"
              className="cursor-pointer rounded-xl"
              onClick={explorer.pickRootFolder}
              disabled={explorer.isPickingFolder}
            >
              <FolderOpen className="size-4" />
              Open folder
            </Button>
          </div>
        </div>

        {!explorer.currentPath && (
          <div className="flex min-h-[480px] flex-col items-center justify-center gap-4 px-6 py-12 text-center">
            <div className="flex size-18 items-center justify-center rounded-[2rem] bg-primary/10 text-primary shadow-inner">
              <FolderOpen className="size-9" />
            </div>
            <div>
              <h3 className="text-xl font-semibold text-foreground">
                Start from a folder on disk
              </h3>
              <p className="mt-2 max-w-md text-sm text-muted-foreground">
                Choose a folder to load a clean, modern explorer view with themed icons.
              </p>
            </div>
            <Button
              className="cursor-pointer rounded-full px-5"
              onClick={explorer.pickRootFolder}
              disabled={explorer.isPickingFolder}
            >
              <FolderOpen className="size-4" />
              Choose folder
            </Button>
          </div>
        )}

        {explorer.currentPath && (
          <>
            {explorer.error && (
              <div className="mx-5 mt-5 rounded-2xl border border-destructive/20 bg-destructive/8 px-4 py-3 text-sm text-destructive">
                {explorer.error}
              </div>
            )}

            <ScrollArea className="min-h-0 flex-1 px-3 py-3">
              <div className="space-y-1 pb-3">
                {explorer.isLoading && explorer.entries.length === 0 ? (
                  <div className="flex min-h-[320px] items-center justify-center gap-3 text-sm text-muted-foreground">
                    <LoaderCircle className="size-4 animate-spin" />
                    Loading folder contents...
                  </div>
                ) : explorer.entries.length === 0 ? (
                  <div className="flex min-h-[320px] flex-col items-center justify-center gap-3 text-center text-sm text-muted-foreground">
                    <FolderOpen className="size-8 text-muted-foreground/80" />
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
                          'group cursor-pointer flex w-full items-center gap-4 rounded-2xl border px-4 py-3 text-left transition-colors',
                          isSelected
                            ? 'border-primary/20 bg-selection text-selection-foreground'
                            : 'border-transparent hover:border-border/70 hover:bg-surface-elevated/85'
                        )}
                      >
                        <div className="flex size-11 items-center justify-center rounded-2xl bg-background/80 shadow-sm ring-1 ring-border/60">
                          <FileIcon
                            name={entry.name}
                            isDirectory={entry.isDirectory}
                            open={isSelected && entry.isDirectory}
                            className="size-5"
                          />
                        </div>

                        <div className="min-w-0 flex-1">
                          <p className="truncate text-sm font-medium">{entry.name}</p>
                          <p className="mt-0.5 text-xs text-muted-foreground group-hover:text-current/70">
                            {entry.isDirectory
                              ? 'Folder · double-click to open'
                              : entry.extension
                                ? `${entry.extension.toUpperCase()} file`
                                : 'File'}
                          </p>
                        </div>

                        {entry.isDirectory && (
                          <ChevronRight className="size-4 shrink-0 text-muted-foreground group-hover:text-current/70" />
                        )}
                      </button>
                    )
                  })
                )}
              </div>
            </ScrollArea>

            <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border/60 px-5 py-3 text-xs text-muted-foreground">
              <span>
                {explorer.entries.length} items · {explorer.directoryEntries.length} folders ·{' '}
                {explorer.fileEntries.length} files
              </span>
              <span>Double-click folders to navigate</span>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

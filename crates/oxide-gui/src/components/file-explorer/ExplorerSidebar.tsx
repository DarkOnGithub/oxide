import { FolderOpen, HardDrive, Sparkles } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { FileIcon } from './FileIcon'
import type { FileExplorerState } from './use-file-explorer'

interface ExplorerSidebarProps {
  explorer: FileExplorerState
}

export function ExplorerSidebar({ explorer }: ExplorerSidebarProps) {
  return (
    <div className="flex h-full min-h-0 flex-col px-4 py-4">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <p className="text-xs font-medium uppercase tracking-[0.24em] text-muted-foreground">
            Oxide
          </p>
          <h2 className="mt-1 text-lg font-semibold text-foreground">
            File explorer
          </h2>
        </div>

        <Button
          size="sm"
          className="cursor-pointer rounded-full px-3 shadow-sm"
          onClick={explorer.pickRootFolder}
          disabled={explorer.isPickingFolder}
        >
          <FolderOpen className="size-4" />
          Open
        </Button>
      </div>

      <div className="mb-4 rounded-2xl border border-border/70 bg-surface-elevated/80 p-4 shadow-sm">
        <div className="flex items-center gap-3">
          <div className="flex size-10 items-center justify-center rounded-2xl bg-primary/10 text-primary">
            <HardDrive className="size-5" />
          </div>

          <div className="min-w-0 flex-1">
            <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
              Workspace
            </p>
            <p className="truncate text-sm font-medium text-foreground">
              {explorer.rootName ?? 'No folder opened'}
            </p>
          </div>
        </div>

        <p className="mt-3 line-clamp-2 text-xs text-muted-foreground">
          {explorer.rootPath ?? 'Choose a starting folder on disk to begin browsing.'}
        </p>

        {explorer.rootPath && explorer.currentPath !== explorer.rootPath && (
          <Button
            variant="outline"
            size="sm"
            className="mt-3 w-full cursor-pointer rounded-xl"
            onClick={() => {
              if (explorer.rootPath) {
                void explorer.openPath(explorer.rootPath)
              }
            }}
          >
            Jump to root
          </Button>
        )}
      </div>

      <div className="mb-2 flex items-center justify-between px-1">
        <p className="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">
          Folders here
        </p>
        <span className="text-xs text-muted-foreground">
          {explorer.directoryEntries.length}
        </span>
      </div>

      <ScrollArea className="min-h-0 flex-1 pr-1">
        <div className="space-y-1.5 pb-4">
          {!explorer.currentPath && (
            <div className="rounded-2xl border border-dashed border-border/80 bg-surface-muted/80 p-4 text-sm text-muted-foreground">
              Open a folder to populate the sidebar.
            </div>
          )}

          {explorer.currentPath && explorer.directoryEntries.length === 0 && (
            <div className="rounded-2xl border border-dashed border-border/80 bg-surface-muted/80 p-4 text-sm text-muted-foreground">
              This folder does not contain any subdirectories.
            </div>
          )}

          {explorer.directoryEntries.map(directory => {
            const isActive = explorer.currentPath === directory.path

            return (
              <button
                key={directory.path}
                type="button"
                onClick={() => explorer.openEntry(directory)}
                className={cn(
                  'cursor-pointer group flex w-full items-center gap-3 rounded-2xl border px-3 py-2.5 text-left transition-colors',
                  isActive
                    ? 'border-primary/20 bg-selection text-selection-foreground'
                    : 'border-transparent bg-transparent text-foreground hover:border-border/70 hover:bg-surface-elevated/80'
                )}
              >
                <FileIcon
                  name={directory.name}
                  isDirectory
                  open={isActive}
                  className="size-5"
                />

                <div className="min-w-0 flex-1">
                  <p className="truncate text-sm font-medium">{directory.name}</p>
                  <p className="truncate text-xs text-muted-foreground group-hover:text-current/65">
                    Folder
                  </p>
                </div>
              </button>
            )
          })}
        </div>
      </ScrollArea>

      <div className="mt-4 rounded-2xl border border-border/70 bg-gradient-to-br from-primary/8 via-surface-elevated to-surface p-4 shadow-sm">
        <div className="flex items-center gap-2 text-sm font-medium text-foreground">
          <Sparkles className="size-4 text-primary" />
          Ready for archives
        </div>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          This minimal explorer is the base for the upcoming archiver integration.
        </p>
      </div>
    </div>
  )
}

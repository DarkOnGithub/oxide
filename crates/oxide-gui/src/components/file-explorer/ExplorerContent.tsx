import { ChevronRight, FolderOpen, LoaderCircle, RefreshCw, Undo2 } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Button } from '@/components/ui/button'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { ArchiveCreationDialog } from './ArchiveCreationDialog'
import { FileIcon } from './FileIcon'
import type { ExplorerEntry, FileExplorerState } from './use-file-explorer'

interface ExplorerContentProps {
  explorer: FileExplorerState
}

interface ContextMenuState {
  entry: ExplorerEntry
  x: number
  y: number
}

// Virtualization constants
const ROW_HEIGHT = 52 // Height of each row in pixels (py-3 + content)
const OVERSCAN_COUNT = 5 // Number of extra rows to render above/below viewport
const MIN_ROWS_FOR_VIRTUALIZATION = 50 // Only virtualize if more than this many rows

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

function entryTypeLabel(entry: ExplorerEntry) {
  if (entry.isDirectory) {
    return 'Folder'
  }

  if (entry.isOxideArchive) {
    return 'Oxide archive'
  }

  return entry.extension ? `${entry.extension.toUpperCase()} File` : 'File'
}

// Virtual list hook for efficient rendering of large lists
function useVirtualList<T>(items: T[], rowHeight: number, overscan: number) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [scrollTop, setScrollTop] = useState(0)
  const [viewportHeight, setViewportHeight] = useState(0)

  const shouldVirtualize = items.length >= MIN_ROWS_FOR_VIRTUALIZATION

  useEffect(() => {
    if (!shouldVirtualize) return

    const scrollContainer = scrollRef.current?.querySelector('[data-radix-scroll-area-viewport]')
    if (!scrollContainer) return

    const handleScroll = () => {
      setScrollTop(scrollContainer.scrollTop)
    }

    const resizeObserver = new ResizeObserver(entries => {
      for (const entry of entries) {
        setViewportHeight(entry.contentRect.height)
      }
    })

    // Initial measurements
    setViewportHeight(scrollContainer.clientHeight)
    setScrollTop(scrollContainer.scrollTop)

    scrollContainer.addEventListener('scroll', handleScroll, { passive: true })
    resizeObserver.observe(scrollContainer)

    return () => {
      scrollContainer.removeEventListener('scroll', handleScroll)
      resizeObserver.disconnect()
    }
  }, [shouldVirtualize])

  const virtualItems = useMemo(() => {
    if (!shouldVirtualize) {
      return items.map((item, index) => ({
        item,
        index,
        style: {},
      }))
    }

    const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan)
    const endIndex = Math.min(
      items.length,
      Math.ceil((scrollTop + viewportHeight) / rowHeight) + overscan
    )

    return items.slice(startIndex, endIndex).map((item, i) => ({
      item,
      index: startIndex + i,
      style: {
        position: 'absolute' as const,
        top: (startIndex + i) * rowHeight,
        height: rowHeight,
        left: 0,
        right: 0,
      },
    }))
  }, [items, scrollTop, viewportHeight, rowHeight, overscan, shouldVirtualize])

  const totalHeight = items.length * rowHeight
  return {
    scrollRef,
    virtualItems,
    totalHeight,
    shouldVirtualize,
  }
}

export function ExplorerContent({ explorer }: ExplorerContentProps) {
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null)
  const contextMenuRef = useRef<HTMLDivElement | null>(null)

  // Setup virtualization for entries
  const {
    scrollRef,
    virtualItems,
    totalHeight,
    shouldVirtualize,
  } = useVirtualList(explorer.entries, ROW_HEIGHT, OVERSCAN_COUNT)

  useEffect(() => {
    if (!contextMenu) {
      return
    }

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target

      if (
        contextMenuRef.current &&
        target instanceof Node &&
        !contextMenuRef.current.contains(target)
      ) {
        setContextMenu(null)
      }
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setContextMenu(null)
      }
    }

    window.addEventListener('pointerdown', handlePointerDown)
    window.addEventListener('keydown', handleKeyDown)

    return () => {
      window.removeEventListener('pointerdown', handlePointerDown)
      window.removeEventListener('keydown', handleKeyDown)
    }
  }, [contextMenu])

  const isEmptyState = explorer.mode === 'filesystem' && !explorer.currentPath

  // Handle context menu for virtualized rows
  const handleContextMenu = useCallback((
    event: React.MouseEvent,
    entry: ExplorerEntry
  ) => {
    if (explorer.mode !== 'filesystem') {
      return
    }

    if (!entry.isDirectory && !entry.isOxideArchive) {
      return
    }

    event.preventDefault()
    event.stopPropagation()
    explorer.setSelectedEntryPath(entry.path)

    const menuWidth = 220
    const menuHeight = entry.isOxideArchive ? 170 : 52
    setContextMenu({
      entry,
      x: Math.min(event.clientX, window.innerWidth - menuWidth),
      y: Math.min(event.clientY, window.innerHeight - menuHeight),
    })
  }, [explorer])

  return (
    <div className="relative flex h-full min-h-0 flex-col bg-surface/20 px-3 py-3">
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
                disabled={!explorer.currentDisplayPath || explorer.isRefreshing}
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
                  <div key={crumb.path || '__root__'} className="flex min-w-0 items-center gap-1.5">
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
                {explorer.currentDisplayPath ?? 'Select a folder'}
              </span>
            )}
          </div>

        </div>

        {isEmptyState ? (
          <div className="flex min-h-0 flex-1 items-center justify-center p-8 text-center">
            <div className="max-w-sm">
              <FolderOpen className="mx-auto size-10 text-muted-foreground/70" />
              <h3 className="mt-4 text-lg font-semibold text-foreground">
                Open a folder to begin
              </h3>
              <p className="mt-2 text-sm text-muted-foreground">
                Browse directories, archive folders, and open Oxide archives like folders.
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

            <ScrollArea ref={scrollRef} className="min-h-0 flex-1">
              <div className="px-2 py-1">
                {explorer.isLoading && explorer.entries.length === 0 ? (
                  <div className="flex min-h-[360px] items-center justify-center gap-3 text-sm text-muted-foreground">
                    <LoaderCircle className="size-4 animate-spin" />
                    Loading contents...
                  </div>
                ) : explorer.entries.length === 0 ? (
                  <div className="flex min-h-[360px] items-center justify-center text-sm text-muted-foreground">
                    This folder is empty.
                  </div>
                ) : (
                  <div
                    style={shouldVirtualize ? {
                      height: totalHeight,
                      position: 'relative',
                    } : undefined}
                  >
                    {virtualItems.map(({ item: entry, style }) => {
                      const isSelected = explorer.selectedEntryPath === entry.path

                      return (
                        <button
                          key={entry.path}
                          type="button"
                          onClick={() => explorer.setSelectedEntryPath(entry.path)}
                          onDoubleClick={() => void explorer.openEntry(entry)}
                          onContextMenu={event => handleContextMenu(event, entry)}
                          className={cn(
                            'grid w-full grid-cols-[minmax(0,1fr)_92px_120px_140px] items-center gap-4 border-b border-border/30 px-4 py-3 text-left text-sm transition-colors',
                            isSelected
                              ? 'bg-selection/70 text-selection-foreground'
                              : 'bg-transparent hover:bg-surface-elevated/70',
                            shouldVirtualize && 'absolute'
                          )}
                          style={shouldVirtualize ? style : undefined}
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
                            {entryTypeLabel(entry)}
                          </span>

                          <span className="truncate text-muted-foreground">
                            {formatDate(entry.modifiedAt)}
                          </span>
                        </button>
                      )
                    })}
                  </div>
                )}
              </div>
              <ScrollBar />
            </ScrollArea>

            <div className="flex items-center justify-between gap-2 border-t border-border/40 px-4 py-2 text-xs text-muted-foreground">
              <span>
                {explorer.entries.length} items · {explorer.directoryEntries.length} folders ·{' '}
                {explorer.fileEntries.length} files
              </span>
              <span>
                {explorer.mode === 'archive'
                  ? 'Browsing archive index'
                  : 'Right-click folders or archives for actions'}
              </span>
            </div>
          </>
        )}
      </div>

      {contextMenu && (
        <div
          ref={contextMenuRef}
          className="fixed z-50 min-w-52 rounded-xl border border-border/60 bg-background/95 p-1.5 shadow-xl backdrop-blur-xl"
          style={{ left: contextMenu.x, top: contextMenu.y }}
          onContextMenu={event => event.preventDefault()}
        >
          {contextMenu.entry.isDirectory && (
            <ContextMenuItem
              label="Archive folder…"
              onClick={() => void explorer.archiveFolder(contextMenu.entry)}
              onClose={() => setContextMenu(null)}
            />
          )}

          {contextMenu.entry.isOxideArchive && (
            <>
              <ContextMenuItem
                label="Open archive"
                onClick={() => void explorer.openArchive(contextMenu.entry.path)}
                onClose={() => setContextMenu(null)}
              />
              <ContextMenuItem
                label="Extract here"
                onClick={() =>
                  void explorer.extractArchiveEntry(contextMenu.entry, 'here')
                }
                onClose={() => setContextMenu(null)}
              />
              <ContextMenuItem
                label="Extract to…"
                onClick={() =>
                  void explorer.extractArchiveEntry(contextMenu.entry, 'choose')
                }
                onClose={() => setContextMenu(null)}
              />
              <ContextMenuItem
                label="Extract here and delete"
                onClick={() =>
                  void explorer.extractArchiveEntry(contextMenu.entry, 'here-delete')
                }
                onClose={() => setContextMenu(null)}
                destructive
              />
            </>
          )}
        </div>
      )}

      {explorer.operation && (
        <div className="absolute inset-0 z-40 flex items-center justify-center bg-background/35 backdrop-blur-[2px]">
          <div className="w-full max-w-md rounded-2xl border border-border/60 bg-background/95 p-5 shadow-2xl">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <p className="text-base font-semibold text-foreground">
                  {explorer.operation.title}
                </p>
                <p className="mt-1 break-all text-sm text-muted-foreground">
                  {explorer.operation.detail}
                </p>
                {explorer.operation.secondaryDetail && (
                  <p className="mt-1 break-all text-xs text-muted-foreground/80">
                    {explorer.operation.secondaryDetail}
                  </p>
                )}
              </div>

              <div className="flex shrink-0 items-center gap-2 text-sm text-muted-foreground">
                <LoaderCircle className="size-4 animate-spin" />
                <span>{Math.round(explorer.operation.progress)}%</span>
              </div>
            </div>

            <div className="mt-4 h-2.5 overflow-hidden rounded-full bg-primary/10">
              <div
                className="h-full rounded-full bg-primary transition-[width] duration-200 ease-out"
                style={{ width: `${explorer.operation.progress}%` }}
              />
            </div>

            <p className="mt-3 text-xs text-muted-foreground">
              Please wait while the archive operation completes.
            </p>
          </div>
        </div>
      )}

      <ArchiveCreationDialog explorer={explorer} />
    </div>
  )
}

function ContextMenuItem({
  label,
  onClick,
  onClose,
  destructive = false,
}: {
  label: string
  onClick: () => void
  onClose: () => void
  destructive?: boolean
}) {
  return (
    <button
      type="button"
      className={cn(
        'flex w-full rounded-lg px-3 py-2 text-left text-sm transition-colors hover:bg-surface-elevated',
        destructive ? 'text-destructive' : 'text-foreground'
      )}
      onClick={() => {
        onClose()
        onClick()
      }}
    >
      {label}
    </button>
  )
}

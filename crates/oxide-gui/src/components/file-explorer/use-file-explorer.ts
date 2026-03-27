import { dirname, join } from '@tauri-apps/api/path'
import { open } from '@tauri-apps/plugin-dialog'
import { readDir, stat, type FileInfo } from '@tauri-apps/plugin-fs'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'
import { logger } from '@/lib/logger'

export interface ExplorerEntry {
  name: string
  path: string
  extension: string | null
  isDirectory: boolean
  isFile: boolean
  isSymlink: boolean
}

export interface ExplorerBreadcrumb {
  label: string
  path: string
}

function getExtension(name: string) {
  const lastDot = name.lastIndexOf('.')

  if (lastDot <= 0 || lastDot === name.length - 1) {
    return null
  }

  return name.slice(lastDot + 1).toLowerCase()
}

function getLeafName(path: string | null) {
  if (!path) {
    return null
  }

  const trimmed = path.replace(/[\\/]+$/, '')

  if (!trimmed) {
    return path
  }

  const segments = trimmed.split(/[\\/]+/)
  return segments.at(-1) ?? path
}

function buildBreadcrumbs(
  rootPath: string | null,
  currentPath: string | null
): ExplorerBreadcrumb[] {
  if (!rootPath || !currentPath) {
    return []
  }

  const rootLabel = getLeafName(rootPath) ?? rootPath
  const breadcrumbs: ExplorerBreadcrumb[] = [{ label: rootLabel, path: rootPath }]

  if (currentPath === rootPath) {
    return breadcrumbs
  }

  const relativePath = currentPath.replace(rootPath, '').replace(/^[\\/]+/, '')

  if (!relativePath) {
    return breadcrumbs
  }

  let accumulatedPath = rootPath
  const separator = rootPath.includes('\\') ? '\\' : '/'

  for (const segment of relativePath.split(/[\\/]+/)) {
    accumulatedPath = `${accumulatedPath}${accumulatedPath.endsWith('\\') || accumulatedPath.endsWith('/') ? '' : separator}${segment}`
    breadcrumbs.push({ label: segment, path: accumulatedPath })
  }

  return breadcrumbs
}

function sortEntries(a: ExplorerEntry, b: ExplorerEntry) {
  if (a.isDirectory !== b.isDirectory) {
    return a.isDirectory ? -1 : 1
  }

  return a.name.localeCompare(b.name, undefined, {
    numeric: true,
    sensitivity: 'base',
  })
}

export function useFileExplorer() {
  const [rootPath, setRootPath] = useState<string | null>(null)
  const [currentPath, setCurrentPath] = useState<string | null>(null)
  const [entries, setEntries] = useState<ExplorerEntry[]>([])
  const [selectedEntryPath, setSelectedEntryPath] = useState<string | null>(null)
  const [selectedEntryInfo, setSelectedEntryInfo] = useState<FileInfo | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [isPickingFolder, setIsPickingFolder] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const loadDirectory = useCallback(
    async (
      path: string,
      options: { markAsRoot?: boolean; preserveSelection?: boolean } = {}
    ) => {
      setIsLoading(true)
      setError(null)

      try {
        const directoryEntries = await readDir(path)

        const nextEntries = await Promise.all(
          directoryEntries.map(async entry => ({
            name: entry.name,
            path: await join(path, entry.name),
            extension: getExtension(entry.name),
            isDirectory: entry.isDirectory,
            isFile: entry.isFile,
            isSymlink: entry.isSymlink,
          }))
        )

        const sortedEntries = nextEntries.sort(sortEntries)

        setEntries(sortedEntries)
        setCurrentPath(path)

        if (options.markAsRoot) {
          setRootPath(path)
        }

        setSelectedEntryPath(previousSelection => {
          if (!options.preserveSelection || !previousSelection) {
            return null
          }

          return sortedEntries.some(entry => entry.path === previousSelection)
            ? previousSelection
            : null
        })
      } catch (loadError) {
        const message =
          loadError instanceof Error
            ? loadError.message
            : 'Failed to load folder contents'

        logger.error('Failed to load directory', { error: loadError, path })
        setError(message)
        toast.error('Unable to open folder', { description: message })
      } finally {
        setIsLoading(false)
      }
    },
    []
  )

  const pickRootFolder = useCallback(async () => {
    setIsPickingFolder(true)

    try {
      const selectedPath = await open({
        directory: true,
        multiple: false,
        title: 'Open folder',
      })

      if (typeof selectedPath === 'string') {
        await loadDirectory(selectedPath, { markAsRoot: true })
      }
    } catch (selectionError) {
      const message =
        selectionError instanceof Error
          ? selectionError.message
          : 'Failed to select folder'

      logger.error('Failed to pick root folder', { error: selectionError })
      toast.error('Unable to select folder', { description: message })
    } finally {
      setIsPickingFolder(false)
    }
  }, [loadDirectory])

  const openEntry = useCallback(
    async (entry: ExplorerEntry) => {
      if (!entry.isDirectory) {
        return
      }

      await loadDirectory(entry.path)
    },
    [loadDirectory]
  )

  const openPath = useCallback(
    async (path: string) => {
      await loadDirectory(path)
    },
    [loadDirectory]
  )

  const refresh = useCallback(async () => {
    if (!currentPath) {
      return
    }

    setIsRefreshing(true)

    try {
      await loadDirectory(currentPath, { preserveSelection: true })
    } finally {
      setIsRefreshing(false)
    }
  }, [currentPath, loadDirectory])

  const goToParent = useCallback(async () => {
    if (!currentPath || !rootPath || currentPath === rootPath) {
      return
    }

    const parentPath = await dirname(currentPath)

    if (parentPath.length >= rootPath.length) {
      await loadDirectory(parentPath)
    }
  }, [currentPath, loadDirectory, rootPath])

  useEffect(() => {
    if (!selectedEntryPath) {
      setSelectedEntryInfo(null)
      return
    }

    let isMounted = true

    stat(selectedEntryPath)
      .then(fileInfo => {
        if (isMounted) {
          setSelectedEntryInfo(fileInfo)
        }
      })
      .catch(selectionError => {
        logger.warn('Failed to load selected entry details', {
          error: selectionError,
          path: selectedEntryPath,
        })

        if (isMounted) {
          setSelectedEntryInfo(null)
        }
      })

    return () => {
      isMounted = false
    }
  }, [selectedEntryPath])

  const selectedEntry = useMemo(
    () => entries.find(entry => entry.path === selectedEntryPath) ?? null,
    [entries, selectedEntryPath]
  )

  const directoryEntries = useMemo(
    () => entries.filter(entry => entry.isDirectory),
    [entries]
  )

  const fileEntries = useMemo(
    () => entries.filter(entry => entry.isFile),
    [entries]
  )

  const breadcrumbs = useMemo(
    () => buildBreadcrumbs(rootPath, currentPath),
    [currentPath, rootPath]
  )

  return {
    rootPath,
    rootName: getLeafName(rootPath),
    currentPath,
    currentName: getLeafName(currentPath),
    entries,
    breadcrumbs,
    directoryEntries,
    fileEntries,
    selectedEntry,
    selectedEntryPath,
    selectedEntryInfo,
    error,
    isLoading,
    isRefreshing,
    isPickingFolder,
    canGoUp: Boolean(rootPath && currentPath && currentPath !== rootPath),
    pickRootFolder,
    openEntry,
    openPath,
    goToParent,
    refresh,
    setSelectedEntryPath,
  }
}

export type FileExplorerState = ReturnType<typeof useFileExplorer>

import { dirname, join } from '@tauri-apps/api/path'
import { open } from '@tauri-apps/plugin-dialog'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'
import {
  getArchivePresetDefaults,
  normalizeArchiveName,
} from '@/lib/archive-presets'
import {
  createOxideArchive,
  extractOxideArchive,
  getPathMetadata,
  listDirectoryEntries,
  readOxideArchiveIndex,
  type ArchiveEntryRecord,
  type ExplorerEntryMetadata,
} from '@/lib/file-explorer'
import { logger } from '@/lib/logger'
import type { CreateArchiveOptions } from '@/lib/tauri-bindings'

export interface ExplorerEntry {
  name: string
  path: string
  displayPath: string
  extension: string | null
  isDirectory: boolean
  isFile: boolean
  isSymlink: boolean
  isOxideArchive: boolean
  size: number
  modifiedAt: Date | null
  mode: number | null
  uid: number | null
  gid: number | null
  target: string | null
  readonly: boolean | null
}

export interface ExplorerBreadcrumb {
  label: string
  path: string
}

interface ArchiveContext {
  archivePath: string
  archiveName: string
  currentArchivePath: string
  hostDirectoryPath: string | null
  manifestEntries: ArchiveEntryRecord[]
}

interface ExplorerOperation {
  title: string
  detail: string
  secondaryDetail?: string
  progress: number
}

interface ArchiveDialogState {
  sourceEntry: ExplorerEntry
  outputDirectory: string
  archiveName: string
  options: CreateArchiveOptions
  advancedOpen: boolean
}

type ArchiveActionMode = 'choose' | 'here' | 'here-delete'

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

  const trimmed = path.replace(/[\/]+$/, '')

  if (!trimmed) {
    return path
  }

  const segments = trimmed.split(/[\/]+/)
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

  const relativePath = currentPath.replace(rootPath, '').replace(/^[\/]+/, '')

  if (!relativePath) {
    return breadcrumbs
  }

  let accumulatedPath = rootPath
  const separator = rootPath.includes('\\') ? '\\' : '/'

  for (const segment of relativePath.split(/[\/]+/)) {
    accumulatedPath = `${accumulatedPath}${accumulatedPath.endsWith('\\') || accumulatedPath.endsWith('/') ? '' : separator}${segment}`
    breadcrumbs.push({ label: segment, path: accumulatedPath })
  }

  return breadcrumbs
}

function buildArchiveBreadcrumbs(
  archiveName: string,
  currentArchivePath: string
): ExplorerBreadcrumb[] {
  const breadcrumbs: ExplorerBreadcrumb[] = [{ label: archiveName, path: '' }]

  if (!currentArchivePath) {
    return breadcrumbs
  }

  let accumulatedPath = ''

  for (const segment of currentArchivePath.split('/')) {
    accumulatedPath = accumulatedPath ? `${accumulatedPath}/${segment}` : segment
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

function archiveDisplayPath(archiveName: string, path: string) {
  return path ? `${archiveName}/${path}` : archiveName
}

function archiveNameFromPath(path: string) {
  return (getLeafName(path) ?? path).replace(/\.oxz$/i, '')
}

function mapFilesystemEntry(entry: Awaited<ReturnType<typeof listDirectoryEntries>>[number]): ExplorerEntry {
  return {
    name: entry.name,
    path: entry.path,
    displayPath: entry.path,
    extension: getExtension(entry.name),
    isDirectory: entry.isDirectory,
    isFile: entry.isFile,
    isSymlink: entry.isSymlink,
    isOxideArchive: entry.isOxideArchive,
    size: entry.size,
    modifiedAt: entry.modifiedAt,
    mode: null,
    uid: null,
    gid: null,
    target: null,
    readonly: null,
  }
}

function mapArchiveRecordToEntry(
  archiveName: string,
  record: ArchiveEntryRecord
): ExplorerEntry {
  const name = getLeafName(record.path) ?? record.path

  return {
    name,
    path: record.path,
    displayPath: archiveDisplayPath(archiveName, record.path),
    extension: getExtension(name),
    isDirectory: record.isDirectory,
    isFile: record.isFile,
    isSymlink: record.isSymlink,
    isOxideArchive: false,
    size: record.size,
    modifiedAt: record.modifiedAt,
    mode: record.mode,
    uid: record.uid,
    gid: record.gid,
    target: record.target ?? null,
    readonly: null,
  }
}

function buildArchiveVisibleEntries(
  archiveName: string,
  manifestEntries: ArchiveEntryRecord[],
  currentArchivePath: string
) {
  const prefix = currentArchivePath ? `${currentArchivePath}/` : ''
  const entries = new Map<string, ExplorerEntry>()

  for (const record of manifestEntries) {
    if (currentArchivePath && record.path === currentArchivePath) {
      continue
    }

    if (prefix && !record.path.startsWith(prefix)) {
      continue
    }

    const remainingPath = prefix ? record.path.slice(prefix.length) : record.path

    if (!remainingPath) {
      continue
    }

    const separatorIndex = remainingPath.indexOf('/')

    if (separatorIndex === -1) {
      entries.set(record.path, mapArchiveRecordToEntry(archiveName, record))
      continue
    }

    const childName = remainingPath.slice(0, separatorIndex)
    const childPath = currentArchivePath
      ? `${currentArchivePath}/${childName}`
      : childName

    if (!entries.has(childPath)) {
      entries.set(childPath, {
        name: childName,
        path: childPath,
        displayPath: archiveDisplayPath(archiveName, childPath),
        extension: null,
        isDirectory: true,
        isFile: false,
        isSymlink: false,
        isOxideArchive: false,
        size: 0,
        modifiedAt: null,
        mode: null,
        uid: null,
        gid: null,
        target: null,
        readonly: null,
      })
    }
  }

  return Array.from(entries.values()).sort(sortEntries)
}

function archiveEntryToMetadata(entry: ExplorerEntry): ExplorerEntryMetadata {
  return {
    isDirectory: entry.isDirectory,
    isFile: entry.isFile,
    isSymlink: entry.isSymlink,
    size: entry.size,
    modifiedAt: entry.modifiedAt,
    accessedAt: null,
    createdAt: null,
    readonly: entry.readonly,
    mode: entry.mode,
    uid: entry.uid,
    gid: entry.gid,
    target: entry.target,
  }
}

function archiveDirectoryMetadata(
  context: ArchiveContext,
  currentArchivePath: string
): ExplorerEntryMetadata {
  const currentEntry = context.manifestEntries.find(
    entry => entry.path === currentArchivePath
  )

  if (currentEntry) {
    return archiveEntryToMetadata(
      mapArchiveRecordToEntry(context.archiveName, currentEntry)
    )
  }

  return {
    isDirectory: true,
    isFile: false,
    isSymlink: false,
    size: 0,
    modifiedAt: null,
    accessedAt: null,
    createdAt: null,
    readonly: null,
    mode: null,
    uid: null,
    gid: null,
    target: null,
  }
}

export function useFileExplorer() {
  const [rootPath, setRootPath] = useState<string | null>(null)
  const [currentPath, setCurrentPath] = useState<string | null>(null)
  const [filesystemEntries, setFilesystemEntries] = useState<ExplorerEntry[]>([])
  const [selectedEntryPath, setSelectedEntryPath] = useState<string | null>(null)
  const [filesystemSelectedEntryInfo, setFilesystemSelectedEntryInfo] =
    useState<ExplorerEntryMetadata | null>(null)
  const [archiveContext, setArchiveContext] = useState<ArchiveContext | null>(null)
  const [archiveDialog, setArchiveDialog] = useState<ArchiveDialogState | null>(null)
  const [operation, setOperation] = useState<ExplorerOperation | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [isPickingFolder, setIsPickingFolder] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!operation) {
      return
    }

    const timer = window.setInterval(() => {
      setOperation(current => {
        if (!current) {
          return null
        }

        return {
          ...current,
          progress: Math.min(current.progress + 8, 92),
        }
      })
    }, 220)

    return () => {
      window.clearInterval(timer)
    }
  }, [operation])

  const startOperation = useCallback(
    (title: string, detail: string, secondaryDetail?: string) => {
      setOperation({
        title,
        detail,
        secondaryDetail,
        progress: 14,
      })
    },
    []
  )

  const finishOperation = useCallback(() => {
    setOperation(null)
  }, [])

  const openArchiveDialog = useCallback(async (entry: ExplorerEntry) => {
    if (!entry.isDirectory || archiveContext) {
      return
    }

    const outputDirectory = await dirname(entry.path)

    setArchiveDialog({
      sourceEntry: entry,
      outputDirectory,
      archiveName: `${entry.name}.oxz`,
      options: getArchivePresetDefaults('Balanced'),
      advancedOpen: false,
    })
  }, [archiveContext])

  const closeArchiveDialog = useCallback(() => {
    setArchiveDialog(null)
  }, [])

  const chooseArchiveOutputDirectory = useCallback(async () => {
    const selectedDirectory = await open({
      directory: true,
      multiple: false,
      title: 'Choose archive destination',
    })

    if (typeof selectedDirectory !== 'string') {
      return
    }

    setArchiveDialog(current =>
      current
        ? {
            ...current,
            outputDirectory: selectedDirectory,
          }
        : current
    )
  }, [])

  const setArchiveDialogName = useCallback((archiveName: string) => {
    setArchiveDialog(current =>
      current
        ? {
            ...current,
            archiveName,
          }
        : current
    )
  }, [])

  const setArchiveDialogAdvancedOpen = useCallback((advancedOpen: boolean) => {
    setArchiveDialog(current =>
      current
        ? {
            ...current,
            advancedOpen,
          }
        : current
    )
  }, [])

  const setArchiveDialogOptions = useCallback((options: CreateArchiveOptions) => {
    setArchiveDialog(current =>
      current
        ? {
            ...current,
            options,
          }
        : current
    )
  }, [])

  const loadDirectory = useCallback(
    async (
      path: string,
      options: { markAsRoot?: boolean; preserveSelection?: boolean } = {}
    ) => {
      setIsLoading(true)
      setError(null)

      try {
        const directoryEntries = await listDirectoryEntries(path)
        const nextEntries = directoryEntries.map(mapFilesystemEntry).sort(sortEntries)

        setFilesystemEntries(nextEntries)
        setCurrentPath(path)
        setArchiveContext(null)

        if (options.markAsRoot) {
          setRootPath(path)
        }

        setSelectedEntryPath(previousSelection => {
          if (!options.preserveSelection || !previousSelection) {
            return nextEntries[0]?.path ?? null
          }

          return nextEntries.some(entry => entry.path === previousSelection)
            ? previousSelection
            : (nextEntries[0]?.path ?? null)
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

  const navigateArchive = useCallback(
    (context: ArchiveContext, nextArchivePath: string, preserveSelection = false) => {
      const visibleEntries = buildArchiveVisibleEntries(
        context.archiveName,
        context.manifestEntries,
        nextArchivePath
      )

      setArchiveContext({ ...context, currentArchivePath: nextArchivePath })
      setSelectedEntryPath(previousSelection => {
        if (preserveSelection && previousSelection) {
          return visibleEntries.some(entry => entry.path === previousSelection)
            ? previousSelection
            : (visibleEntries[0]?.path ?? null)
        }

        return visibleEntries[0]?.path ?? null
      })
    },
    []
  )

  const openArchive = useCallback(
    async (archivePath: string) => {
      setIsLoading(true)
      setError(null)

      try {
        const index = await readOxideArchiveIndex(archivePath)
        const context: ArchiveContext = {
          archivePath,
          archiveName: archiveNameFromPath(archivePath),
          currentArchivePath: '',
          hostDirectoryPath: currentPath,
          manifestEntries: index.entries,
        }

        navigateArchive(context, '')
      } catch (openError) {
        const message =
          openError instanceof Error
            ? openError.message
            : 'Failed to open archive'

        logger.error('Failed to open archive', { error: openError, path: archivePath })
        setError(message)
        toast.error('Unable to open archive', { description: message })
      } finally {
        setIsLoading(false)
      }
    },
    [currentPath, navigateArchive]
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
      if (archiveContext) {
        if (entry.isDirectory) {
          navigateArchive(archiveContext, entry.path)
        }

        return
      }

      if (entry.isDirectory) {
        await loadDirectory(entry.path)
        return
      }

      if (entry.isOxideArchive) {
        await openArchive(entry.path)
      }
    },
    [archiveContext, loadDirectory, navigateArchive, openArchive]
  )

  const openPath = useCallback(
    async (path: string) => {
      if (archiveContext) {
        navigateArchive(archiveContext, path, true)
        return
      }

      await loadDirectory(path)
    },
    [archiveContext, loadDirectory, navigateArchive]
  )

  const refresh = useCallback(async () => {
    if (archiveContext) {
      setIsRefreshing(true)

      try {
        const index = await readOxideArchiveIndex(archiveContext.archivePath)
        navigateArchive(
          {
            ...archiveContext,
            manifestEntries: index.entries,
          },
          archiveContext.currentArchivePath,
          true
        )
      } catch (refreshError) {
        const message =
          refreshError instanceof Error
            ? refreshError.message
            : 'Failed to refresh archive'

        logger.error('Failed to refresh archive', {
          error: refreshError,
          path: archiveContext.archivePath,
        })
        toast.error('Unable to refresh archive', { description: message })
      } finally {
        setIsRefreshing(false)
      }

      return
    }

    if (!currentPath) {
      return
    }

    setIsRefreshing(true)

    try {
      await loadDirectory(currentPath, { preserveSelection: true })
    } finally {
      setIsRefreshing(false)
    }
  }, [archiveContext, currentPath, loadDirectory, navigateArchive])

  const goToParent = useCallback(async () => {
    if (archiveContext) {
      if (archiveContext.currentArchivePath) {
        const parentPath = archiveContext.currentArchivePath.includes('/')
          ? archiveContext.currentArchivePath.slice(
              0,
              archiveContext.currentArchivePath.lastIndexOf('/')
            )
          : ''

        navigateArchive(archiveContext, parentPath, true)
        return
      }

      setArchiveContext(null)
      setSelectedEntryPath(archiveContext.archivePath)
      return
    }

    if (!currentPath || !rootPath || currentPath === rootPath) {
      return
    }

    const parentPath = await dirname(currentPath)

    if (parentPath.length >= rootPath.length) {
      await loadDirectory(parentPath)
    }
  }, [archiveContext, currentPath, loadDirectory, navigateArchive, rootPath])

  const archiveFolder = useCallback(async (entry: ExplorerEntry) => {
    if (!entry.isDirectory || archiveContext) {
      return
    }

    await openArchiveDialog(entry)
  }, [archiveContext, openArchiveDialog])

  const confirmArchiveDialog = useCallback(async () => {
    if (!archiveDialog) {
      return
    }

    const outputPath = await join(
      archiveDialog.outputDirectory,
      normalizeArchiveName(archiveDialog.archiveName)
    )

    try {
      setArchiveDialog(null)
      startOperation('Creating archive', archiveDialog.sourceEntry.path, `Output: ${outputPath}`)
      await createOxideArchive(
        archiveDialog.sourceEntry.path,
        outputPath,
        archiveDialog.options
      )
      toast.success('Archive created', { description: outputPath })

      if (currentPath) {
        await loadDirectory(currentPath, { preserveSelection: true })
      }
    } catch (archiveError) {
      const message =
        archiveError instanceof Error
          ? archiveError.message
          : 'Failed to archive folder'

      logger.error('Failed to archive folder', {
        error: archiveError,
        path: archiveDialog.sourceEntry.path,
      })
      toast.error('Unable to archive folder', { description: message })
    } finally {
      finishOperation()
    }
  }, [archiveDialog, currentPath, finishOperation, loadDirectory, startOperation])

  const extractArchiveEntry = useCallback(
    async (entry: ExplorerEntry, mode: ArchiveActionMode) => {
      if (!entry.isOxideArchive || archiveContext) {
        return
      }

      try {
        let outputDirectory: string | null = null

        if (mode === 'choose') {
          const selectedDirectory = await open({
            directory: true,
            multiple: false,
            title: 'Extract archive to',
          })

          outputDirectory = typeof selectedDirectory === 'string' ? selectedDirectory : null
        } else {
          const parentDirectory = await dirname(entry.path)
          outputDirectory = await join(
            parentDirectory,
            archiveNameFromPath(entry.path)
          )
        }

        if (!outputDirectory) {
          return
        }

        startOperation(
          mode === 'here-delete' ? 'Extracting archive and deleting source' : 'Extracting archive',
          entry.path,
          `Target: ${outputDirectory}`
        )
        await extractOxideArchive(entry.path, outputDirectory, mode === 'here-delete')
        toast.success('Archive extracted', { description: outputDirectory })

        if (currentPath) {
          await loadDirectory(currentPath, { preserveSelection: true })
        }
      } catch (extractError) {
        const message =
          extractError instanceof Error
            ? extractError.message
            : 'Failed to extract archive'

        logger.error('Failed to extract archive', {
          error: extractError,
          path: entry.path,
        })
        toast.error('Unable to extract archive', { description: message })
      } finally {
        finishOperation()
      }
    },
    [archiveContext, currentPath, finishOperation, loadDirectory, startOperation]
  )

  useEffect(() => {
    if (archiveContext) {
      setFilesystemSelectedEntryInfo(null)
      return
    }

    const infoPath = selectedEntryPath ?? currentPath

    if (!infoPath) {
      setFilesystemSelectedEntryInfo(null)
      return
    }

    let isMounted = true

    getPathMetadata(infoPath)
      .then(fileInfo => {
        if (isMounted) {
          setFilesystemSelectedEntryInfo(fileInfo)
        }
      })
      .catch(selectionError => {
        logger.warn('Failed to load selected entry details', {
          error: selectionError,
          path: infoPath,
        })

        if (isMounted) {
          setFilesystemSelectedEntryInfo(null)
        }
      })

    return () => {
      isMounted = false
    }
  }, [archiveContext, currentPath, selectedEntryPath])

  const entries = useMemo(
    () =>
      archiveContext
        ? buildArchiveVisibleEntries(
            archiveContext.archiveName,
            archiveContext.manifestEntries,
            archiveContext.currentArchivePath
          )
        : filesystemEntries,
    [archiveContext, filesystemEntries]
  )

  const selectedEntry = useMemo(
    () => entries.find(entry => entry.path === selectedEntryPath) ?? null,
    [entries, selectedEntryPath]
  )

  const selectedEntryInfo = useMemo(() => {
    if (archiveContext) {
      return selectedEntry
        ? archiveEntryToMetadata(selectedEntry)
        : archiveDirectoryMetadata(archiveContext, archiveContext.currentArchivePath)
    }

    return filesystemSelectedEntryInfo
  }, [archiveContext, filesystemSelectedEntryInfo, selectedEntry])

  const directoryEntries = useMemo(
    () => entries.filter(entry => entry.isDirectory),
    [entries]
  )

  const fileEntries = useMemo(
    () => entries.filter(entry => entry.isFile),
    [entries]
  )

  const breadcrumbs = useMemo(
    () =>
      archiveContext
        ? buildArchiveBreadcrumbs(
            archiveContext.archiveName,
            archiveContext.currentArchivePath
          )
        : buildBreadcrumbs(rootPath, currentPath),
    [archiveContext, currentPath, rootPath]
  )

  const currentDisplayPath = archiveContext
    ? archiveDisplayPath(
        archiveContext.archiveName,
        archiveContext.currentArchivePath
      )
    : (currentPath ?? null)

  const selectedDisplayPath = archiveContext
    ? archiveDisplayPath(
        archiveContext.archiveName,
        selectedEntry?.path ?? archiveContext.currentArchivePath
      )
    : (selectedEntry?.displayPath ?? currentPath ?? '—')

  return {
    mode: archiveContext ? 'archive' : 'filesystem',
    rootPath,
    rootName: getLeafName(rootPath),
    currentPath,
    currentName: archiveContext
      ? getLeafName(archiveContext.currentArchivePath) ?? archiveContext.archiveName
      : getLeafName(currentPath),
    currentDisplayPath,
    selectedDisplayPath,
    archivePath: archiveContext?.archivePath ?? null,
    archiveName: archiveContext?.archiveName ?? null,
    archiveDialog,
    entries,
    breadcrumbs,
    directoryEntries,
    fileEntries,
    selectedEntry,
    selectedEntryPath,
    selectedEntryInfo,
    error,
    operation,
    isLoading,
    isRefreshing,
    isPickingFolder,
    canGoUp: archiveContext
      ? Boolean(archiveContext.currentArchivePath || archiveContext.hostDirectoryPath)
      : Boolean(rootPath && currentPath && currentPath !== rootPath),
    terminalPath: archiveContext ? null : (currentPath ?? rootPath),
    pickRootFolder,
    openEntry,
    openPath,
    openArchive,
    goToParent,
    refresh,
    setSelectedEntryPath,
    closeArchiveDialog,
    chooseArchiveOutputDirectory,
    confirmArchiveDialog,
    setArchiveDialogAdvancedOpen,
    setArchiveDialogName,
    setArchiveDialogOptions,
    archiveFolder,
    extractArchiveEntry,
  }
}

export type FileExplorerState = ReturnType<typeof useFileExplorer>

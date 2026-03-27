import {
  commands,
  unwrapResult,
  type CreateArchiveOptions,
  type ExplorerArchiveEntry,
  type ExplorerArchiveEntryKind,
  type ExplorerArchiveIndex,
  type ExplorerArchiveSourceKind,
  type ExplorerDirectoryEntry,
  type ExplorerPathMetadata,
} from '@/lib/tauri-bindings'

function toDate(value: number | null): Date | null {
  return value === null ? null : new Date(value)
}

export type ExplorerEntryMetadata = Omit<ExplorerPathMetadata, 'modifiedAt' | 'accessedAt' | 'createdAt' | 'readonly'> & {
  modifiedAt: Date | null
  accessedAt: Date | null
  createdAt: Date | null
  readonly: boolean | null
  target?: string | null
}

export type ExplorerEntryRecord = Omit<
  ExplorerDirectoryEntry,
  'modifiedAt'
> & {
  modifiedAt: Date | null
}

export type ArchiveEntryRecord = Omit<
  ExplorerArchiveEntry,
  'modifiedAt'
> & {
  modifiedAt: Date | null
  isDirectory: boolean
  isFile: boolean
  isSymlink: boolean
}

export type ArchiveIndexRecord = Omit<ExplorerArchiveIndex, 'entries'> & {
  sourceKind: ExplorerArchiveSourceKind
  entries: ArchiveEntryRecord[]
}

function kindFlags(kind: ExplorerArchiveEntryKind) {
  return {
    isDirectory: kind === 'Directory',
    isFile: kind === 'File',
    isSymlink: kind === 'Symlink',
  }
}

export async function listDirectoryEntries(
  path: string
): Promise<ExplorerEntryRecord[]> {
  const entries = unwrapResult(await commands.listDirectoryEntries(path))

  return entries.map(entry => ({
    ...entry,
    modifiedAt: toDate(entry.modifiedAt),
  }))
}

export async function getPathMetadata(
  path: string
): Promise<ExplorerEntryMetadata> {
  const metadata = unwrapResult(await commands.getPathMetadata(path))

  return {
    ...metadata,
    modifiedAt: toDate(metadata.modifiedAt),
    accessedAt: toDate(metadata.accessedAt),
    createdAt: toDate(metadata.createdAt),
    readonly: metadata.readonly,
  }
}

export async function isOxideArchive(path: string): Promise<boolean> {
  return unwrapResult(await commands.isOxideArchive(path))
}

export async function readOxideArchiveIndex(
  path: string
): Promise<ArchiveIndexRecord> {
  const index = unwrapResult(await commands.readOxideArchiveIndex(path))

  return {
    sourceKind: index.sourceKind,
    entries: index.entries.map(entry => ({
      ...entry,
      ...kindFlags(entry.kind),
      modifiedAt: toDate(entry.modifiedAt),
    })),
  }
}

export async function createOxideArchive(
  sourcePath: string,
  outputPath: string,
  options: CreateArchiveOptions
): Promise<void> {
  unwrapResult(await commands.createOxideArchive(sourcePath, outputPath, options))
}

export async function extractOxideArchive(
  archivePath: string,
  outputDirectory: string,
  deleteSource = false
): Promise<void> {
  unwrapResult(
    await commands.extractOxideArchive(archivePath, outputDirectory, deleteSource)
  )
}

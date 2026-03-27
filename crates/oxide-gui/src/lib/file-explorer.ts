import {
  commands,
  unwrapResult,
  type ExplorerDirectoryEntry,
  type ExplorerPathMetadata,
} from '@/lib/tauri-bindings'

function toDate(value: number | null): Date | null {
  return value === null ? null : new Date(value)
}

export type ExplorerEntryMetadata = Omit<ExplorerDirectoryEntry, 'modifiedAt'> & {
  modifiedAt: Date | null
}

export type ExplorerPathMetadataValue = Omit<
  ExplorerPathMetadata,
  'modifiedAt' | 'accessedAt' | 'createdAt'
> & {
  modifiedAt: Date | null
  accessedAt: Date | null
  createdAt: Date | null
}

export async function listDirectoryEntries(
  path: string
): Promise<ExplorerEntryMetadata[]> {
  const entries = unwrapResult(await commands.listDirectoryEntries(path))

  return entries.map(entry => ({
    ...entry,
    modifiedAt: toDate(entry.modifiedAt),
  }))
}

export async function getPathMetadata(
  path: string
): Promise<ExplorerPathMetadataValue> {
  const metadata = unwrapResult(await commands.getPathMetadata(path))

  return {
    ...metadata,
    modifiedAt: toDate(metadata.modifiedAt),
    accessedAt: toDate(metadata.accessedAt),
    createdAt: toDate(metadata.createdAt),
  }
}

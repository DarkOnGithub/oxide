import type {
  ArchiveCompressionAlgo,
  ArchiveDictionaryModeOption,
  ArchivePreset,
  CreateArchiveOptions,
} from '@/lib/tauri-bindings'

export interface ArchivePresetDefinition {
  id: ArchivePreset
  label: string
  description: string
  summary: string
  defaults: CreateArchiveOptions
}

const MIB = 1024 * 1024

export const ARCHIVE_PRESETS: ArchivePresetDefinition[] = [
  {
    id: 'Fast',
    label: 'Fast',
    description: 'Minimal compression, best speed',
    summary: 'LZ4 · 2 MiB blocks',
    defaults: {
      preset: 'Fast',
      compressionAlgo: 'Lz4',
      compressionLevel: null,
      dictionaryMode: 'Auto',
      blockSize: 2 * MIB,
      workers: 0,
      producerThreads: 1,
      lzmaExtreme: false,
      lzmaDictionarySize: null,
    },
  },
  {
    id: 'Balanced',
    label: 'Balanced',
    description: 'Default ratio vs speed',
    summary: 'Zstd level 6 · 2 MiB blocks',
    defaults: {
      preset: 'Balanced',
      compressionAlgo: 'Zstd',
      compressionLevel: 6,
      dictionaryMode: 'Auto',
      blockSize: 2 * MIB,
      workers: 0,
      producerThreads: 3,
      lzmaExtreme: false,
      lzmaDictionarySize: null,
    },
  },
  {
    id: 'Ultra',
    label: 'Ultra',
    description: 'Higher compression for storage',
    summary: 'LZMA level 7 · 2 MiB dict',
    defaults: {
      preset: 'Ultra',
      compressionAlgo: 'Lzma',
      compressionLevel: 7,
      dictionaryMode: 'Auto',
      blockSize: 2 * MIB,
      workers: 0,
      producerThreads: 1,
      lzmaExtreme: false,
      lzmaDictionarySize: 2 * MIB,
    },
  },
  {
    id: 'Extreme',
    label: 'Extreme',
    description: 'Maximum compression, slower archiving',
    summary: 'LZMA level 9 · 4 MiB blocks',
    defaults: {
      preset: 'Extreme',
      compressionAlgo: 'Lzma',
      compressionLevel: 9,
      dictionaryMode: 'Auto',
      blockSize: 4 * MIB,
      workers: 0,
      producerThreads: 1,
      lzmaExtreme: false,
      lzmaDictionarySize: 4 * MIB,
    },
  },
]

export function getArchivePresetDefinition(preset: ArchivePreset) {
  return (
    ARCHIVE_PRESETS.find(item => item.id === preset) ??
    ARCHIVE_PRESETS.find(item => item.id === 'Balanced')!
  )
}

export function getArchivePresetDefaults(preset: ArchivePreset): CreateArchiveOptions {
  return { ...getArchivePresetDefinition(preset).defaults }
}

export function formatBytesAsMiB(bytes: number) {
  return Math.max(1, Math.round(bytes / MIB))
}

export function mibToBytes(value: number) {
  return Math.max(1, Math.round(value)) * MIB
}

export const COMPRESSION_ALGORITHM_OPTIONS: Array<{
  value: ArchiveCompressionAlgo
  label: string
}> = [
  { value: 'Lz4', label: 'LZ4' },
  { value: 'Zstd', label: 'Zstandard' },
  { value: 'Lzma', label: 'LZMA' },
]

export const DICTIONARY_MODE_OPTIONS: Array<{
  value: ArchiveDictionaryModeOption
  label: string
}> = [
  { value: 'Auto', label: 'Auto' },
  { value: 'Off', label: 'Off' },
]

export function normalizeArchiveName(name: string) {
  const trimmed = name.trim()
  if (!trimmed) {
    return 'archive.oxz'
  }

  return trimmed.toLowerCase().endsWith('.oxz') ? trimmed : `${trimmed}.oxz`
}

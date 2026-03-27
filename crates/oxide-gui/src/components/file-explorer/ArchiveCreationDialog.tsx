import type { ReactNode } from 'react'
import {
  ARCHIVE_PRESETS,
  COMPRESSION_ALGORITHM_OPTIONS,
  DICTIONARY_MODE_OPTIONS,
  formatBytesAsMiB,
  getArchivePresetDefaults,
  mibToBytes,
  normalizeArchiveName,
} from '@/lib/archive-presets'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { NativeSelect, NativeSelectOption } from '@/components/ui/native-select'
import { Switch } from '@/components/ui/switch'
import { cn } from '@/lib/utils'
import type { FileExplorerState } from './use-file-explorer'

interface ArchiveCreationDialogProps {
  explorer: FileExplorerState
}

export function ArchiveCreationDialog({ explorer }: ArchiveCreationDialogProps) {
  const dialog = explorer.archiveDialog
  const options = dialog?.options

  return (
    <Dialog open={Boolean(dialog)} onOpenChange={open => !open && explorer.closeArchiveDialog()}>
      {dialog && options && (
        <DialogContent className="max-w-4xl rounded-[22px] border-border/50 bg-background/98 p-0 shadow-2xl backdrop-blur-xl">
          <div className="border-b border-border/40 px-8 py-6">
            <DialogHeader className="text-left">
              <DialogTitle className="text-2xl font-semibold tracking-tight">
                Create archive
              </DialogTitle>
              <DialogDescription className="text-sm text-muted-foreground">
                Configure how{' '}
                <span className="font-medium text-foreground">{dialog.sourceEntry.name}</span>{' '}
                should be archived.
              </DialogDescription>
            </DialogHeader>
          </div>

          <div className="space-y-8 px-8 py-7">
            <section className="grid gap-6 lg:grid-cols-[minmax(0,0.95fr)_minmax(0,1.25fr)]">
              <div className="space-y-2">
                <Label htmlFor="archive-name" className="text-xs font-medium tracking-wide text-muted-foreground">
                  Archive name
                </Label>
                <Input
                  id="archive-name"
                  value={dialog.archiveName}
                  onChange={event => explorer.setArchiveDialogName(event.target.value)}
                  placeholder="folder.oxz"
                  className="h-11 rounded-xl"
                />
              </div>

              <div className="space-y-2">
                <Label
                  htmlFor="archive-output-directory"
                  className="text-xs font-medium tracking-wide text-muted-foreground"
                >
                  Destination folder
                </Label>
                <div className="flex gap-3">
                  <Input
                    id="archive-output-directory"
                    value={dialog.outputDirectory}
                    readOnly
                    className="h-11 rounded-xl font-mono text-xs"
                  />
                  <Button
                    type="button"
                    variant="outline"
                    className="h-11 rounded-xl px-5"
                    onClick={() => void explorer.chooseArchiveOutputDirectory()}
                  >
                    Choose…
                  </Button>
                </div>
              </div>
            </section>

            <section className="space-y-3">
              <div className="text-sm font-medium text-foreground">
                Presets
              </div>

              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {ARCHIVE_PRESETS.map(preset => {
                  const isActive = options.preset === preset.id

                  return (
                    <button
                      key={preset.id}
                      type="button"
                      onClick={() => explorer.setArchiveDialogOptions(getArchivePresetDefaults(preset.id))}
                      className={cn(
                        'rounded-xl border px-4 py-3 text-left text-sm font-medium transition-colors',
                        isActive
                          ? 'border-foreground bg-foreground text-background shadow-sm'
                          : 'border-border/60 bg-background/60 text-foreground hover:border-foreground/30 hover:bg-accent/20'
                      )}
                    >
                      {preset.label}
                    </button>
                  )
                })}
              </div>
            </section>

            <section className="rounded-2xl border border-border/50 bg-muted/15 p-5">
              <button
                type="button"
                className="flex w-full items-center justify-between gap-3 text-left"
                onClick={() => explorer.setArchiveDialogAdvancedOpen(!dialog.advancedOpen)}
              >
                <div>
                  <p className="text-sm font-medium text-foreground">Advanced settings</p>
                  <p className="text-xs text-muted-foreground">
                    Override compressor, level, block size, and worker counts.
                  </p>
                </div>

                <span className="text-xs font-medium text-muted-foreground">
                  {dialog.advancedOpen ? 'Hide' : 'Show'}
                </span>
              </button>

              {dialog.advancedOpen && (
                <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                  <Field label="Compression algorithm">
                    <NativeSelect
                      value={options.compressionAlgo}
                      onChange={event =>
                        explorer.setArchiveDialogOptions({
                          ...options,
                          compressionAlgo: event.target.value as typeof options.compressionAlgo,
                          compressionLevel:
                            event.target.value === 'Lz4'
                              ? null
                              : options.compressionLevel,
                          lzmaExtreme:
                            event.target.value === 'Lzma' ? options.lzmaExtreme : false,
                          lzmaDictionarySize:
                            event.target.value === 'Lzma'
                              ? options.lzmaDictionarySize
                              : null,
                        })
                      }
                      className="w-full"
                    >
                      {COMPRESSION_ALGORITHM_OPTIONS.map(option => (
                        <NativeSelectOption key={option.value} value={option.value}>
                          {option.label}
                        </NativeSelectOption>
                      ))}
                    </NativeSelect>
                  </Field>

                  <Field label="Compression level">
                    <Input
                      type="number"
                      min={options.compressionAlgo === 'Lzma' ? 1 : 1}
                      max={options.compressionAlgo === 'Lzma' ? 9 : 22}
                      value={options.compressionLevel ?? ''}
                      onChange={event =>
                        explorer.setArchiveDialogOptions({
                          ...options,
                          compressionLevel:
                            event.target.value === ''
                              ? null
                              : Number(event.target.value),
                        })
                      }
                      disabled={options.compressionAlgo === 'Lz4'}
                      className="h-10 rounded-xl"
                    />
                  </Field>

                  <Field label="Dictionary mode">
                    <NativeSelect
                      value={options.dictionaryMode}
                      onChange={event =>
                        explorer.setArchiveDialogOptions({
                          ...options,
                          dictionaryMode: event.target.value as typeof options.dictionaryMode,
                        })
                      }
                      className="w-full"
                    >
                      {DICTIONARY_MODE_OPTIONS.map(option => (
                        <NativeSelectOption key={option.value} value={option.value}>
                          {option.label}
                        </NativeSelectOption>
                      ))}
                    </NativeSelect>
                  </Field>

                  <Field label="Block size (MiB)">
                    <Input
                      type="number"
                      min={1}
                      value={formatBytesAsMiB(options.blockSize)}
                      onChange={event =>
                        explorer.setArchiveDialogOptions({
                          ...options,
                          blockSize: mibToBytes(Number(event.target.value || 1)),
                        })
                      }
                      className="h-10 rounded-xl"
                    />
                  </Field>

                  <Field label="Workers (0 = auto)">
                    <Input
                      type="number"
                      min={0}
                      value={options.workers}
                      onChange={event =>
                        explorer.setArchiveDialogOptions({
                          ...options,
                          workers: Number(event.target.value || 0),
                        })
                      }
                      className="h-10 rounded-xl"
                    />
                  </Field>

                  <Field label="Producer threads (0 = preset)">
                    <Input
                      type="number"
                      min={0}
                      value={options.producerThreads}
                      onChange={event =>
                        explorer.setArchiveDialogOptions({
                          ...options,
                          producerThreads: Number(event.target.value || 0),
                        })
                      }
                      className="h-10 rounded-xl"
                    />
                  </Field>

                  {options.compressionAlgo === 'Lzma' && (
                    <>
                      <Field label="LZMA dictionary size (MiB)">
                        <Input
                          type="number"
                          min={1}
                          value={
                            options.lzmaDictionarySize
                              ? formatBytesAsMiB(options.lzmaDictionarySize)
                              : ''
                          }
                          onChange={event =>
                            explorer.setArchiveDialogOptions({
                              ...options,
                              lzmaDictionarySize:
                                event.target.value === ''
                                  ? null
                                  : mibToBytes(Number(event.target.value || 1)),
                            })
                          }
                          className="h-10 rounded-xl"
                        />
                      </Field>

                      <div className="flex items-center justify-between rounded-xl border border-border/60 bg-background/60 px-3 py-2.5 xl:col-span-2">
                        <div>
                          <Label htmlFor="lzma-extreme">LZMA extreme mode</Label>
                          <p className="mt-1 text-xs text-muted-foreground">
                            Push LZMA harder for a smaller archive, at the cost of speed.
                          </p>
                        </div>

                        <Switch
                          id="lzma-extreme"
                          checked={options.lzmaExtreme}
                          onCheckedChange={checked =>
                            explorer.setArchiveDialogOptions({
                              ...options,
                              lzmaExtreme: checked,
                            })
                          }
                        />
                      </div>
                    </>
                  )}
                </div>
              )}
            </section>

            <div className="rounded-2xl border border-border/50 bg-background/60 px-5 py-4 text-sm text-muted-foreground">
              <p className="text-xs font-medium tracking-wide text-foreground">Archive output</p>
              <p className="mt-2 break-all font-mono text-xs leading-5">
                {`${dialog.outputDirectory}/${normalizeArchiveName(dialog.archiveName)}`}
              </p>
            </div>
          </div>

          <DialogFooter className="border-t border-border/40 px-8 py-5">
            <Button
              type="button"
              variant="ghost"
              className="rounded-xl px-5"
              onClick={explorer.closeArchiveDialog}
            >
              Cancel
            </Button>
            <Button
              type="button"
              className="rounded-xl px-5"
              onClick={() => void explorer.confirmArchiveDialog()}
            >
              Create archive
            </Button>
          </DialogFooter>
        </DialogContent>
      )}
    </Dialog>
  )
}

function Field({
  label,
  children,
}: {
  label: string
  children: ReactNode
}) {
  return (
    <div className="space-y-2">
      <Label className="text-xs font-medium tracking-wide text-muted-foreground">
        {label}
      </Label>
      {children}
    </div>
  )
}

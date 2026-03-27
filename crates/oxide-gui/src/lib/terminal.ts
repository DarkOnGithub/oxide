import { commands, unwrapResult } from '@/lib/tauri-bindings'

export async function openTerminalInFolder(path: string): Promise<void> {
  unwrapResult(await commands.openTerminalInCurrentFolder(path))
}

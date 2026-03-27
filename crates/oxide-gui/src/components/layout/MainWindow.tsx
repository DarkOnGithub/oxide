import {
  ResizablePanelGroup,
  ResizablePanel,
  ResizableHandle,
} from '@/components/ui/resizable'
import { TitleBar } from '@/components/titlebar/TitleBar'
import { RightSideBar } from './RightSideBar'
import { MainWindowContent } from './MainWindowContent'
import { CommandPalette } from '@/components/command-palette/CommandPalette'
import { PreferencesDialog } from '@/components/preferences/PreferencesDialog'
import { Toaster } from 'sonner'
import { useTheme } from '@/hooks/use-theme'
import { useUIStore } from '@/store/ui-store'
import { useMainWindowEventListeners } from '@/hooks/useMainWindowEventListeners'
import { ExplorerContent } from '@/components/file-explorer/ExplorerContent'
import { ExplorerDetails } from '@/components/file-explorer/ExplorerDetails'
import { useFileExplorer } from '@/components/file-explorer/use-file-explorer'

/**
 * Layout sizing configuration for resizable panels.
 * All values are percentages of total width.
 * Right sidebar default + main default must equal 100.
 */
const LAYOUT = {
  rightSidebar: { default: 24, min: 18, max: 34 },
  main: { min: 36 },
} as const

// Main content default is calculated to ensure totals sum to 100%
const MAIN_CONTENT_DEFAULT = 100 - LAYOUT.rightSidebar.default

export function MainWindow() {
  const { resolvedTheme } = useTheme()
  const rightSidebarVisible = useUIStore(state => state.rightSidebarVisible)
  const explorer = useFileExplorer()

  // Set up global event listeners (keyboard shortcuts, etc.)
  useMainWindowEventListeners()

  return (
    <div className="flex h-screen w-full flex-col overflow-hidden bg-background/95">
      <TitleBar />

      <div className="flex flex-1 overflow-hidden">
        {rightSidebarVisible ? (
          <ResizablePanelGroup direction="horizontal">
            <ResizablePanel
              defaultSize={MAIN_CONTENT_DEFAULT}
              minSize={LAYOUT.main.min}
            >
              <MainWindowContent className="flex-1 bg-transparent">
                <ExplorerContent explorer={explorer} />
              </MainWindowContent>
            </ResizablePanel>

            <ResizableHandle />

            <ResizablePanel
              defaultSize={LAYOUT.rightSidebar.default}
              minSize={LAYOUT.rightSidebar.min}
              maxSize={LAYOUT.rightSidebar.max}
            >
              <RightSideBar className="border-border/50 bg-surface/55 backdrop-blur-xl">
                <ExplorerDetails explorer={explorer} />
              </RightSideBar>
            </ResizablePanel>
          </ResizablePanelGroup>
        ) : (
          <MainWindowContent className="flex-1 bg-transparent">
            <ExplorerContent explorer={explorer} />
          </MainWindowContent>
        )}
      </div>

      {/* Global UI Components (hidden until triggered) */}
      <CommandPalette />
      <PreferencesDialog />
      <Toaster
        position="bottom-right"
        theme={resolvedTheme}
        className="toaster group"
        toastOptions={{
          classNames: {
            toast:
              'group toast group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-lg',
            description: 'group-[.toast]:text-muted-foreground',
            actionButton:
              'group-[.toast]:bg-primary group-[.toast]:text-primary-foreground',
            cancelButton:
              'group-[.toast]:bg-muted group-[.toast]:text-muted-foreground',
          },
        }}
      />
    </div>
  )
}

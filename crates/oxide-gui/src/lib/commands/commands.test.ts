import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import type { TFunction } from 'i18next'
import type { CommandContext, AppCommand } from './types'

const mockUIStore = {
  getState: vi.fn(() => ({
    rightSidebarVisible: true,
    commandPaletteOpen: false,
    setRightSidebarVisible: vi.fn(),
  })),
}

vi.mock('@/store/ui-store', () => ({
  useUIStore: mockUIStore,
}))

const { registerCommands, getAllCommands, executeCommand } =
  await import('./registry')
const { navigationCommands } = await import('./navigation-commands')

const createMockContext = (): CommandContext => ({
  openPreferences: vi.fn(),
  showToast: vi.fn(),
})

// Mock translation function for testing
const mockT = ((key: string): string => {
  const translations: Record<string, string> = {
    'commands.showRightSidebar.label': 'Show Right Sidebar',
    'commands.showRightSidebar.description': 'Show the right sidebar',
    'commands.hideRightSidebar.label': 'Hide Right Sidebar',
    'commands.hideRightSidebar.description': 'Hide the right sidebar',
    'commands.openPreferences.label': 'Open Preferences',
    'commands.openPreferences.description': 'Open the application preferences',
  }
  return translations[key] || key
}) as TFunction

describe('Simplified Command System', () => {
  let mockContext: CommandContext

  beforeEach(() => {
    mockContext = createMockContext()
    registerCommands(navigationCommands)
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  describe('Command Registration', () => {
    it('registers commands correctly', () => {
      const commands = getAllCommands(mockContext)
      expect(commands.length).toBeGreaterThan(0)

      const sidebarCommand = commands.find(
        cmd => cmd.id === 'show-right-sidebar' || cmd.id === 'hide-right-sidebar'
      )
      expect(sidebarCommand).toBeDefined()
      expect(mockT(sidebarCommand?.labelKey ?? '')).toContain('Sidebar')
    })

    it('filters commands by availability', () => {
      mockUIStore.getState.mockReturnValue({
        rightSidebarVisible: false,
        commandPaletteOpen: false,
        setRightSidebarVisible: vi.fn(),
      })

      const availableCommands = getAllCommands(mockContext)
      const showSidebarCommand = availableCommands.find(
        cmd => cmd.id === 'show-right-sidebar'
      )
      const hideSidebarCommand = availableCommands.find(
        cmd => cmd.id === 'hide-right-sidebar'
      )

      expect(showSidebarCommand).toBeDefined()
      expect(hideSidebarCommand).toBeUndefined()
    })

    it('filters commands by search term using translations', () => {
      const searchResults = getAllCommands(mockContext, 'sidebar', mockT)

      expect(searchResults.length).toBeGreaterThan(0)
      searchResults.forEach(cmd => {
        const label = mockT(cmd.labelKey).toLowerCase()
        const description = cmd.descriptionKey
          ? mockT(cmd.descriptionKey).toLowerCase()
          : ''
        const matchesSearch =
          label.includes('sidebar') || description.includes('sidebar')

        expect(matchesSearch).toBe(true)
      })
    })
  })

  describe('Command Execution', () => {
    it('executes show-right-sidebar command correctly', async () => {
      mockUIStore.getState.mockReturnValue({
        rightSidebarVisible: false,
        commandPaletteOpen: false,
        setRightSidebarVisible: vi.fn(),
      })

      const result = await executeCommand('show-right-sidebar', mockContext)

      expect(result.success).toBe(true)
    })

    it('fails to execute unavailable command', async () => {
      mockUIStore.getState.mockReturnValue({
        rightSidebarVisible: true,
        commandPaletteOpen: false,
        setRightSidebarVisible: vi.fn(),
      })

      const result = await executeCommand('show-right-sidebar', mockContext)

      expect(result.success).toBe(false)
      expect(result.error).toContain('not available')
    })

    it('handles non-existent command', async () => {
      const result = await executeCommand('non-existent-command', mockContext)

      expect(result.success).toBe(false)
      expect(result.error).toContain('not found')
    })

    it('handles command execution errors', async () => {
      const errorCommand: AppCommand = {
        id: 'error-command',
        labelKey: 'commands.error.label',
        execute: () => {
          throw new Error('Test error')
        },
      }

      registerCommands([errorCommand])

      const result = await executeCommand('error-command', mockContext)

      expect(result.success).toBe(false)
      expect(result.error).toContain('Test error')
    })
  })
})

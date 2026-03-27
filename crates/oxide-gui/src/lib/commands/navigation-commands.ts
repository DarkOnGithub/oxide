import { PanelRight, Settings } from 'lucide-react'
import { useUIStore } from '@/store/ui-store'
import type { AppCommand } from './types'

export const navigationCommands: AppCommand[] = [
  {
    id: 'show-right-sidebar',
    labelKey: 'commands.showRightSidebar.label',
    descriptionKey: 'commands.showRightSidebar.description',
    icon: PanelRight,
    group: 'navigation',
    shortcut: '⌘+2',
    keywords: ['sidebar', 'right', 'panel', 'show'],

    execute: () => {
      useUIStore.getState().setRightSidebarVisible(true)
    },

    isAvailable: () => !useUIStore.getState().rightSidebarVisible,
  },

  {
    id: 'hide-right-sidebar',
    labelKey: 'commands.hideRightSidebar.label',
    descriptionKey: 'commands.hideRightSidebar.description',
    icon: PanelRight,
    group: 'navigation',
    shortcut: '⌘+2',
    keywords: ['sidebar', 'right', 'panel', 'hide'],

    execute: () => {
      useUIStore.getState().setRightSidebarVisible(false)
    },

    isAvailable: () => useUIStore.getState().rightSidebarVisible,
  },

  {
    id: 'open-preferences',
    labelKey: 'commands.openPreferences.label',
    descriptionKey: 'commands.openPreferences.description',
    icon: Settings,
    group: 'settings',
    shortcut: '⌘+,',
    keywords: ['preferences', 'settings', 'config', 'options'],

    execute: context => {
      context.openPreferences()
    },
  },
]

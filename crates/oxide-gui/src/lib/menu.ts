/**
 * Application menu builder using Tauri's JavaScript API.
 *
 * This module creates native menus from JavaScript, enabling i18n support
 * through react-i18next. Menus are rebuilt when the language changes.
 */
import { logger } from '@/lib/logger'
import { Menu } from '@tauri-apps/api/menu'

/**
 * Build and set the application menu with translated labels.
 */
export async function buildAppMenu(): Promise<Menu> {
  try {
    // Keep an empty app menu so native menu entries don't appear.
    const menu = await Menu.new({ items: [] })

    // Set as the application menu
    await menu.setAsAppMenu()

    logger.info('Application menu built successfully')
    return menu
  } catch (error) {
    logger.error('Failed to build application menu', { error })
    throw error
  }
}

/**
 * No-op listener retained for app startup compatibility.
 */
export function setupMenuLanguageListener(): () => void {
  return () => {}
}

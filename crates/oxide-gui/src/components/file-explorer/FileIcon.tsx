import { File, Folder } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTheme } from '@/hooks/use-theme'
import { resolveIconTheme } from '@/lib/theme'

const rawIconModules = import.meta.glob(
  '../../../assets/vscode-icons/icons/{latte,frappe,macchiato,mocha}/*.svg',
  {
    eager: true,
    import: 'default',
  }
) as Record<string, string>

const iconModules = Object.entries(rawIconModules).reduce<
  Record<string, string>
>((accumulator, [path, url]) => {
  const match = path.match(/icons\/([^/]+)\/([^/]+)\.svg$/)

  if (!match) {
    return accumulator
  }

  const [, theme, iconName] = match
  accumulator[`${theme}/${iconName}`] = url
  return accumulator
}, {})

const folderIcons: Record<string, string> = {
  '.git': 'folder_git',
  '.github': 'folder_github',
  '.cargo': 'folder_cargo',
  assets: 'folder_assets',
  cargo: 'folder_cargo',
  config: 'folder_config',
  docs: 'folder_docs',
  documents: 'folder_docs',
  downloads: 'folder_download',
  images: 'folder_images',
  pictures: 'folder_images',
  public: 'folder_public',
  scripts: 'folder_scripts',
  src: 'folder_src',
  'src-tauri': 'folder_tauri',
}

const fileNameIcons: Record<string, string> = {
  'cargo.lock': 'cargo-lock',
  'cargo.toml': 'cargo',
  '.gitignore': 'git',
  'dockerfile': 'docker',
  'package-lock.json': 'lock',
  'readme.md': 'markdown',
}

const extensionIcons: Record<string, string> = {
  bash: 'bash',
  bin: 'binary',
  css: 'css',
  exe: 'exe',
  gif: 'image',
  go: 'go',
  gz: 'zip',
  html: 'html',
  jpeg: 'image',
  jpg: 'image',
  js: 'javascript',
  json: 'json',
  jsx: 'javascript',
  lock: 'lock',
  markdown: 'markdown',
  md: 'markdown',
  mov: 'video',
  mp3: 'audio',
  mp4: 'video',
  oxz: 'zip',
  pdf: 'pdf',
  png: 'image',
  py: 'python',
  rar: 'zip',
  rs: 'rust',
  sh: 'bash',
  svg: 'svg',
  tar: 'zip',
  text: 'text',
  toml: 'toml',
  ts: 'typescript',
  tsx: 'typescript',
  txt: 'text',
  wav: 'audio',
  webp: 'image',
  yaml: 'yaml',
  yml: 'yaml',
  zip: 'zip',
}

function getIconName(name: string, isDirectory: boolean, open: boolean) {
  const normalizedName = name.toLowerCase()

  if (isDirectory) {
    const folderIcon = folderIcons[normalizedName] ?? '_folder'
    return open ? `${folderIcon}_open` : folderIcon
  }

  const exactMatch = fileNameIcons[normalizedName]

  if (exactMatch) {
    return exactMatch
  }

  const extension = normalizedName.includes('.')
    ? normalizedName.split('.').at(-1)
    : null

  return (extension && extensionIcons[extension]) || '_file'
}

interface FileIconProps {
  name: string
  isDirectory: boolean
  open?: boolean
  className?: string
}

export function FileIcon({
  name,
  isDirectory,
  open = false,
  className,
}: FileIconProps) {
  const { theme, resolvedTheme } = useTheme()
  const iconTheme = resolveIconTheme(theme, resolvedTheme)
  const iconName = getIconName(name, isDirectory, open)
  const iconSource =
    iconModules[`${iconTheme}/${iconName}`] ??
    iconModules[`${iconTheme}/${isDirectory ? '_folder' : '_file'}`]

  if (!iconSource) {
    return isDirectory ? (
      <Folder className={cn('size-4 shrink-0', className)} />
    ) : (
      <File className={cn('size-4 shrink-0', className)} />
    )
  }

  return (
    <img
      src={iconSource}
      alt=""
      aria-hidden="true"
      className={cn('size-4 shrink-0', className)}
    />
  )
}

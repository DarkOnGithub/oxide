import { defineConfig } from 'vitepress'

export default defineConfig({
    title: "Oxide",
    description: "High-performance Rust archiver for files, directories, and .oxz archives",
    appearance: 'dark',
    base: '/oxide/',
    themeConfig: {
        logo: '/logo.svg',
        nav: [
            { text: 'Documentation', link: '/cli/' },
            { text: 'A propos', link: '/about/' }
        ],

        sidebar: {
            '/cli/': [
                {
                    text: 'Prise en main',
                    items: [
                        { text: 'Installation', link: '/cli/' }
                    ]
                },
                {
                    text: 'Commandes',
                    items: [
                        { text: 'Archive', link: '/cli/archive' },
                        { text: 'Extract', link: '/cli/extract' },
                        { text: 'Tree', link: '/cli/tree' },
                        { text: 'Presets', link: '/cli/presets' }
                    ]
                }
            ],
            '/about/': [
                {
                    text: 'À propos',
                    items: [
                        { text: 'Équipe', link: '/about/' },
                        { text: 'Chronologie', link: '/about/timeline' },
                        { text: 'Évolution de la stratégie', link: '/about/strategy-evolution' },
                        { text: 'Dépendances', link: '/about/dependencies' }
                    ]
                }
            ]
        },

        socialLinks: [
            { icon: 'github', link: 'https://github.com/DarkOnGithub/oxide' },
            { icon: 'x', link: 'https://twitter.com/' }
        ],

        search: {
            provider: 'local'
        },

        outline: {
            level: [2, 3],
            label: 'Sur cette page'
        }
    }
})

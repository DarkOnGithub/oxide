import { defineConfig } from 'vitepress'

export default defineConfig({
    title: "Oxide",
    description: "Ergonomic Framework for Humans",
    appearance: 'dark',
    base: '/',
    themeConfig: {
        logo: '/logo.svg', // Will need a logo or can omit if not present
        nav: [
            { text: 'Docs', link: '/documentation/' },
            { text: 'A propos', link: '/a-propos/' }
        ],

        sidebar: {
            '/documentation/': [
                {
                    text: 'Installation',
                    items: [
                        { text: 'Téléchargement', link: '/documentation/' }
                    ]
                },
                {
                    text: 'Utilisation',
                    items: [
                        { text: 'Archive', link: '/documentation/archive' },
                        { text: 'Extract', link: '/documentation/extract' },
                        { text: 'Tree', link: '/documentation/tree' },
                        { text: 'Presets', link: '/documentation/presets' }
                    ]
                }
            ],
            '/a-propos/': [
                {
                    text: 'À propos',
                    items: [
                        { text: 'L\'Équipe', link: '/a-propos/' },
                        { text: 'Évolution de la Stratégie', link: '/a-propos/limitations' },
                        { text: 'Dépendances', link: '/a-propos/dependances' }
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
            label: 'On this page'
        }
    }
})

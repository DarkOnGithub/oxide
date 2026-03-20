import { defineConfig } from 'vitepress'

export default defineConfig({
    title: "Oxide",
    description: "High-performance Rust archiver for files, directories, and .oxz archives",
    appearance: 'dark',
    base: '/oxide/',
    themeConfig: {
        logo: '/logo.svg',
        nav: [
            { text: 'CLI Docs', link: '/cli/' },
            { text: 'About', link: '/about/' }
        ],

        sidebar: {
            '/cli/': [
                {
                    text: 'Getting Started',
                    items: [
                        { text: 'Installation', link: '/cli/' }
                    ]
                },
                {
                    text: 'Commands',
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
                    text: 'About',
                    items: [
                        { text: 'Team', link: '/about/' },
                        { text: 'Timeline', link: '/about/timeline' },
                        { text: 'Strategy Evolution', link: '/about/strategy-evolution' },
                        { text: 'Dependencies', link: '/about/dependencies' }
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

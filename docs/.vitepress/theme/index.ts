import DefaultTheme from 'vitepress/theme'
import './style.css'
import BenchmarkTable from './components/BenchmarkTable.vue'

export default {
    extends: DefaultTheme,
    enhanceApp({ app }) {
        app.component('BenchmarkTable', BenchmarkTable)
    }
}

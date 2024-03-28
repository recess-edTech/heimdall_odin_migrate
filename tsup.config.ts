import {defineConfig} from 'tsup'

export default defineConfig({
    format: ['cjs', 'esm'],
    entry: ['src/script.ts'],
    dts: true,
    shims: true,
    skipNodeModulesBundle: true,
    clean: true,
    sourcemap: true,
})
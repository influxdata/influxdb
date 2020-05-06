import resolve from 'rollup-plugin-node-resolve'
import commonjs from 'rollup-plugin-commonjs'
import sourceMaps from 'rollup-plugin-sourcemaps'
import {terser} from 'rollup-plugin-terser'
import gzip from 'rollup-plugin-gzip'
import typescript from 'rollup-plugin-typescript2'
import tsc from 'typescript'

const pkg = require('./package.json')

let plugins = [
  resolve(),
  commonjs(),
  typescript({typescript: tsc}),
  sourceMaps(),
]

// Minify and compress output when in production
if (process.env.NODE_ENV === 'production') {
  plugins = [...plugins, terser(), gzip()]
}

export default {
  input: 'src/index.ts',
  output: {
    name: pkg.name,
    file: pkg.main,
    format: 'umd',
    sourcemap: true,
    globals: {
      react: 'React',
      'react-dom': 'ReactDOM',
    },
  },
  plugins,
  external: ['react', 'react-dom'], // Do not bundle peer dependencies
}

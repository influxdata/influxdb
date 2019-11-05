export {}
const webpack = require('webpack')
const path = require('path')
const {dependencies} = require('./package.json')
const MonacoWebpackPlugin = require('monaco-editor-webpack-plugin')

// only dll infrequently updated dependencies
const vendor = Object.keys(dependencies).filter(
  d =>
    !d.includes('@influxdata') &&
    !d.includes('webpack-bundle-analyzer') &&
    !d.includes('monaco-editor-webpack-plugin')
)

const MONACO_DIR = path.resolve(__dirname, './node_modules/monaco-editor')

module.exports = {
  context: __dirname,
  mode: 'development',
  entry: {
    vendor,
  },
  output: {
    path: path.join(__dirname, 'build'),
    filename: '[name].bundle.js',
    library: '[name]',
  },
  module: {
    rules: [
      {
        test: /\.css$/,
        include: MONACO_DIR,
        use: ['style-loader', 'css-loader'],
      },
    ],
  },
  plugins: [
    new webpack.DllPlugin({
      name: '[name]',
      path: path.join(__dirname, 'build', '[name]-manifest.json'),
    }),
    new MonacoWebpackPlugin({
      languages: ['json', 'javascript', 'go', 'markdown'],
    }),
  ],
  stats: {
    colors: true,
    children: false,
    modules: false,
    version: false,
    assetsSort: '!size',
    warningsFilter: /export .* was not found in/,
    excludeAssets: [/\.(hot-update|woff|eot|ttf|svg|ico|png)/],
  },
  performance: {hints: false},
}

export {}
const webpack = require('webpack')
const path = require('path')
const {dependencies} = require('./package.json')

// only dll infrequently updated dependencies
const vendor = Object.keys(dependencies).filter(d => (
  !d.includes('@influxdata') &&
  !d.includes('webpack-bundle-analyzer')
))

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
  plugins: [
    new webpack.DllPlugin({
      name: '[name]',
      path: path.join(__dirname, 'build', '[name]-manifest.json'),
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

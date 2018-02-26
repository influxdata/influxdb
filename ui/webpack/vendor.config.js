const path = require('path')
const webpack = require('webpack')
const packages = require('../package.json')
const dependencies = packages.dependencies

module.exports = {
  node: {
    fs: 'empty',
    module: 'empty',
  },
  context: process.cwd(),
  resolve: {
    extensions: ['.js', '.jsx', '.json'],
    modules: [__dirname, 'node_modules'],
  },
  entry: {
    vendor: Object.keys(dependencies),
  },
  output: {
    filename: '[name].dll.js',
    path: path.resolve(__dirname, '../build'),
    library: '[name]',
  },
  plugins: [
    new webpack.DllPlugin({
      name: '[name]',
      path: './build/[name].dll.json',
    }),
  ],
}

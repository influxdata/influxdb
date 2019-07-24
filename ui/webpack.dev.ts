export {}
const merge = require('webpack-merge')
const webpack = require('webpack')
const common = require('./webpack.common.ts')
const path = require('path')

module.exports = merge(common, {
  mode: 'development',
  devtool: 'cheap-inline-source-map',
  devServer: {
    hot: true,
    historyApiFallback: true,
    compress: true,
    proxy: {
      '/api/v2': 'http://127.0.0.1:9999',
      '/debug/flush': 'http://127.0.0.1:9999',
    },
  },
  plugins: [
    new webpack.DllReferencePlugin({
      context: path.join(__dirname, 'build'),
      manifest: require('./build/vendor-manifest.json'),
    }),
  ],
})

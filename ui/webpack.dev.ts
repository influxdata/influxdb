const m = require('webpack-merge')
const c = require('./webpack.common.ts')

module.exports = m(c, {
  mode: 'development',
  devtool: 'inline-source-map',
  devServer: {
    hot: true,
    historyApiFallback: true,
    compress: true,
    proxy: {
      '/api/v2': 'http://localhost:9999',
    },
  },
})

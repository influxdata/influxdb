export {}
const merge = require('webpack-merge')
const common = require('./webpack.common.ts')
const PORT = parseInt(process.env.PORT, 10) || 8080
const PUBLIC = process.env.PUBLIC || undefined
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

module.exports = merge(common, {
  mode: 'development',
  devtool: 'cheap-inline-source-map',
  output: {
    filename: '[name].js',
  },
  devServer: {
    hot: true,
    historyApiFallback: true,
    compress: true,
    proxy: {
      '/api/v2': 'http://localhost:9999',
      '/debug/flush': 'http://localhost:9999',
      '/oauth': 'http://localhost:9999',
    },
    disableHostCheck: true,
    host: '0.0.0.0',
    port: PORT,
    public: PUBLIC,
  },
  plugins: [
    new BundleAnalyzerPlugin({
      analyzerMode: 'server',
      analyzerHost: '0.0.0.0',
      analyzerPort: '9998',
      openAnalyzer: false,
    }),
  ],
})

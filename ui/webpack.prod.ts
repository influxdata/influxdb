export {}

// utils
const common = require('./webpack.common.ts')
const merge = require('webpack-merge')
const path = require('path')

// Plugins
const OptimizeCSSAssetsPlugin = require('optimize-css-assets-webpack-plugin')
const TerserJSPlugin = require('terser-webpack-plugin')
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

const {STATIC_DIRECTORY} = require('./src/utils/env')

module.exports = merge(common, {
  mode: 'production',
  devtool: 'source-map',
  output: {
    filename: `${STATIC_DIRECTORY}[contenthash:10].js`,
  },
  module: {
    rules: [
      {
        test: /\.js$/,
        enforce: 'pre', // this forces this rule to run first.
        use: ['source-map-loader'],
        include: [
          path.resolve(__dirname, 'node_modules/@influxdata/giraffe'),
          path.resolve(__dirname, 'node_modules/@influxdata/clockface'),
        ],
      },
    ],
  },
  optimization: {
    minimizer: [
      new TerserJSPlugin({
        cache: true,
        parallel: 2,
        sourceMap: true,
      }),
      new OptimizeCSSAssetsPlugin({}),
    ],
    splitChunks: {
      chunks: 'all',
    },
  },
  plugins: [
    new BundleAnalyzerPlugin({
      analyzerMode: 'static',
      reportFilename: 'bundle-report.html',
      openAnalyzer: false,
    }),
  ],
})

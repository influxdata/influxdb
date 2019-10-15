export {}

// utils
const common = require('./webpack.common.ts')
const merge = require('webpack-merge')
const path = require('path')

// Plugins
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin')
const MiniCssExtractPlugin = require('mini-css-extract-plugin')
const OptimizeCSSAssetsPlugin = require('optimize-css-assets-webpack-plugin')
const TerserJSPlugin = require('terser-webpack-plugin')

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
        test: /\.s?css$/i,
        use: [
          MiniCssExtractPlugin.loader,
          'css-loader',
          {
            loader: 'sass-loader',
            options: {
              implementation: require('sass'),
              hmr: true,
            },
          },
        ],
      },
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
        parallel: true,
        sourceMap: true,
      }),
      new OptimizeCSSAssetsPlugin({}),
    ],
    splitChunks: {
      chunks: 'all',
    },
  },
  plugins: [
    new MiniCssExtractPlugin({
      filename: `${STATIC_DIRECTORY}[contenthash:10].css`,
      chunkFilename: `${STATIC_DIRECTORY}[id].[contenthash:10].css`,
    }),
    new ForkTsCheckerWebpackPlugin(),
  ],
})

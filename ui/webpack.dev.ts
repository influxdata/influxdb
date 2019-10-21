export {}
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin')
const merge = require('webpack-merge')
const webpack = require('webpack')
const common = require('./webpack.common.ts')
const path = require('path')

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
    },
  },
  module: {
    rules: [
      {
        test: /\.s?css$/i,
        use: [
          'style-loader',
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
    ],
  },
  plugins: [
    new webpack.DllReferencePlugin({
      context: path.join(__dirname, 'build'),
      manifest: require('./build/vendor-manifest.json'),
    }),
    new ForkTsCheckerWebpackPlugin(),
  ],
})

const path = require('path')
const fs = require('fs')
const util = require('util')
const webpack = require('webpack')
const ExtractTextPlugin = require('extract-text-webpack-plugin')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const WebpackOnBuildPlugin = require('on-build-webpack')
const _ = require('lodash')

const readdir = util.promisify(fs.readdir)
const unlink = util.promisify(fs.unlink)

const package = require('../package.json')
const dependencies = package.dependencies

const buildDir = path.resolve(__dirname, '../build')

module.exports = {
  watch: true,
  devtool: 'source-map',
  entry: {
    app: path.resolve(__dirname, '..', 'src', 'index.js'),
    vendor: Object.keys(dependencies),
  },
  output: {
    publicPath: '/',
    path: path.resolve(__dirname, '../build'),
    filename: '[name].[hash].dev.js',
  },
  resolve: {
    alias: {
      src: path.resolve(__dirname, '..', 'src'),
      shared: path.resolve(__dirname, '..', 'src', 'shared'),
      style: path.resolve(__dirname, '..', 'src', 'style'),
      utils: path.resolve(__dirname, '..', 'src', 'utils'),
    },
  },
  module: {
    noParse: [
      path.resolve(
        __dirname,
        '..',
        'node_modules',
        'memoizerific',
        'memoizerific.js'
      ),
    ],
    preLoaders: [
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'eslint-loader',
      },
    ],
    loaders: [
      {
        test: /\.json$/,
        loader: 'json',
      },
      {
        test: /\.scss$/,
        loader: ExtractTextPlugin.extract(
          'style-loader',
          'css-loader!sass-loader!resolve-url!sass?sourceMap'
        ),
      },
      {
        test: /\.css$/,
        loader: ExtractTextPlugin.extract(
          'style-loader',
          'css-loader!postcss-loader'
        ),
      },
      {
        test: /\.(ico|png|cur|jpg|ttf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
        loader: 'file',
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'babel',
        query: {
          presets: ['es2015', 'react', 'stage-0'],
          cacheDirectory: true, // use a cache directory to speed up compilation
        },
      },
    ],
  },
  sassLoader: {
    includePaths: [path.resolve(__dirname, 'node_modules')],
  },
  eslint: {
    failOnWarning: false,
    failOnError: false,
  },
  plugins: [
    new webpack.HotModuleReplacementPlugin(),
    new webpack.ProvidePlugin({
      $: 'jquery',
      jQuery: 'jquery',
    }),
    new ExtractTextPlugin('chronograf.css'),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, '..', 'src', 'index.template.html'),
      inject: 'body',
      favicon: 'assets/images/favicon.ico',
    }),
    new webpack.optimize.CommonsChunkPlugin({
      names: ['vendor', 'manifest'],
    }),
    new webpack.DefinePlugin({
      VERSION: JSON.stringify(require('../package.json').version),
    }),
    new WebpackOnBuildPlugin(async stats => {
      try {
        const newlyCreatedAssets = stats.compilation.assets
        const buildDirFiles = await readdir(buildDir)
        const assetFileNames = _.keys(newlyCreatedAssets)
        const filesToRemove = _.difference(buildDirFiles, assetFileNames)

        for (file of filesToRemove) {
          const ext = path.extname(file)
          if (['.js', '.json', '.map'].includes(ext)) {
            unlink(path.join(buildDir, file))
          }
        }
      } catch (err) {
        console.error(err)
      }
    }),
  ],
  postcss: require('./postcss'),
  target: 'web',
  devServer: {
    hot: true,
    historyApiFallback: true,
    clientLogLevel: 'info',
    stats: {colors: true},
    contentBase: 'build',
    quiet: false,
    watchOptions: {
      aggregateTimeout: 300,
      poll: 1000,
    },
    proxy: {
      '/chronograf/v1': {
        target: 'http://localhost:8888',
        secure: false,
      },
    },
  },
}

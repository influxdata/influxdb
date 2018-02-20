const path = require('path')
const fs = require('fs')
const webpack = require('webpack')
const ExtractTextPlugin = require('extract-text-webpack-plugin')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const WebpackOnBuildPlugin = require('on-build-webpack')
const _ = require('lodash')

const package = require('../package.json')
const dependencies = package.dependencies

const buildDir = path.resolve(__dirname, '../build')
const babelLoader = {
  loader: 'babel-loader',
  options: {
    cacheDirectory: true,
    presets: [
      'react',
      [
        'es2015',
        {
          modules: false,
        },
      ],
      'es2016',
    ],
  },
}

module.exports = {
  watch: true,
  cache: true,
  devtool: 'inline-eval-cheap-source-map',
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
    extensions: ['.ts', '.tsx', '.js'],
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
    loaders: [
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'eslint-loader',
        enforce: 'pre',
      },
      {
        test: /\.scss$/,
        loader: ExtractTextPlugin.extract({
          fallback: 'style-loader',
          use: [
            'css-loader',
            'sass-loader',
            'resolve-url-loader',
            'sass-loader?sourceMap',
          ],
        }),
      },
      {
        test: /\.css$/,
        loader: ExtractTextPlugin.extract({
          fallback: 'style-loader',
          use: ['css-loader', 'postcss-loader'],
        }),
      },
      {
        test: /\.(ico|png|cur|jpg|ttf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
        loader: 'file-loader',
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'babel-loader',
        query: {
          presets: ['es2015', 'react', 'stage-0'],
          cacheDirectory: true, // use a cache directory to speed up compilation
        },
      },
      {
        test: /\.ts(x?)$/,
        exclude: /node_modules/,
        use: [
          babelLoader,
          {
            loader: 'ts-loader',
          },
        ],
      },
    ],
  },
  plugins: [
    new webpack.LoaderOptionsPlugin({
      options: {
        postcss: require('./postcss'),
        sassLoader: {
          includePaths: [path.resolve(__dirname, 'node_modules')],
        },
        eslint: {
          failOnWarning: false,
          failOnError: false,
        },
      },
    }),
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
    new WebpackOnBuildPlugin(stats => {
      const newlyCreatedAssets = stats.compilation.assets
      fs.readdir(buildDir, (readdirErr, buildDirFiles) => {
        if (readdirErr) {
          console.error('webpack build directory error')
          return
        }

        const assetFileNames = _.keys(newlyCreatedAssets)
        const filesToRemove = _.difference(buildDirFiles, assetFileNames)

        for (const file of filesToRemove) {
          const ext = path.extname(file)
          if (['.js', '.json', '.map'].includes(ext)) {
            fs.unlink(path.join(buildDir, file), unlinkErr => {
              if (unlinkErr) {
                console.error('webpack cleanup error', unlinkErr)
              }
            })
          }
        }
      })
    }),
  ],
  target: 'web',
  devServer: {
    hot: true,
    historyApiFallback: true,
    clientLogLevel: 'info',
    stats: {
      colors: true,
    },
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

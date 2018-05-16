const path = require('path')
const fs = require('fs')
const webpack = require('webpack')
const ExtractTextPlugin = require('extract-text-webpack-plugin')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const WebpackOnBuildPlugin = require('on-build-webpack')
const HtmlWebpackIncludeAssetsPlugin = require('html-webpack-include-assets-plugin')
const keys = require('lodash/keys')
const difference = require('lodash/difference')
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin')

const buildDir = path.resolve(__dirname, '../build')

const babelLoader = {
  loader: 'babel-loader',
  options: {
    cacheDirectory: true,
    presets: [
      [
        'env',
        {
          modules: false,
        },
      ],
      'react',
      'stage-0',
    ],
  },
}

const stats = {
  colors: true,
  children: false,
  modules: false,
  version: false,
  assetsSort: '!size',
  excludeAssets: [/\.(hot-update|woff|eot|ttf|svg|ico|png)/],
}

module.exports = {
  stats,
  node: {
    fs: 'empty',
    module: 'empty',
  },
  watch: true,
  cache: true,
  devtool: 'cheap-eval-source-map',
  entry: {
    app: path.resolve(__dirname, '..', 'src', 'index.tsx'),
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
    rules: [
      {
        test: /\.ts(x?)$/,
        exclude: /node_modules/,
        loader: 'tslint-loader',
        enforce: 'pre',
        options: {
          emitWarning: true,
          configFile: path.resolve(__dirname, '..', 'tslint.json'),
        },
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'eslint-loader',
        enforce: 'pre',
        options: {
          emitWarning: true,
        },
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
        include: path.resolve(__dirname, '..', 'src'),
        exclude: /node_modules/,
        use: [
          {
            loader: 'thread-loader',
          },
          {
            loader: 'babel-loader',
            options: {
              presets: ['env', 'react', 'stage-0'],
              plugins: ['transform-decorators-legacy'],
              cacheDirectory: true, // use a cache directory to speed up compilation
            },
          },
        ],
      },
      {
        test: /\.ts(x?)$/,
        exclude: /node_modules/,
        use: [
          {
            loader: 'thread-loader',
            options: {
              // there should be 1 cpu for the fork-ts-checker-webpack-plugin
              workers: require('os').cpus().length - 1,
            },
          },
          babelLoader,
          {
            loader: 'ts-loader',
            options: {
              happyPackMode: true, // required for use with thread-loader
            },
          },
        ],
      },
    ],
  },
  plugins: [
    new webpack.DefinePlugin({
      'process.env.NODE_ENV': JSON.stringify('development'),
    }),
    new webpack.DllReferencePlugin({
      context: process.cwd(),
      manifest: require('../build/vendor.dll.json'),
    }),
    new ForkTsCheckerWebpackPlugin({
      checkSyntacticErrors: true,
    }),
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
    new ExtractTextPlugin('chronograf.css'),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, '..', 'src', 'index.template.html'),
      inject: 'body',
      favicon: 'assets/images/favicon.ico',
    }),
    new HtmlWebpackIncludeAssetsPlugin({
      assets: ['vendor.dll.js'],
      append: false,
    }),
    new webpack.DefinePlugin({
      VERSION: JSON.stringify(require('../package.json').version),
    }),
    new WebpackOnBuildPlugin(webpackStats => {
      const newlyCreatedAssets = webpackStats.compilation.assets
      fs.readdir(buildDir, (readdirErr, buildDirFiles) => {
        if (readdirErr) {
          console.error('webpack build directory error')
          return
        }

        const assetFileNames = keys(newlyCreatedAssets)
        const filesToRemove = difference(buildDirFiles, assetFileNames)

        for (const file of filesToRemove) {
          if (file.includes('dll')) {
            return
          }

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
    stats,
    hot: true,
    historyApiFallback: true,
    clientLogLevel: 'info',
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

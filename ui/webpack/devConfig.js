var webpack = require('webpack')
var path = require('path')
var ExtractTextPlugin = require('extract-text-webpack-plugin')
var HtmlWebpackPlugin = require('html-webpack-plugin')
var package = require('../package.json')
const WebpackOnBuildPlugin = require('on-build-webpack')
const fs = require('fs')
var dependencies = package.dependencies

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
    filename: '[name].[chunkhash].dev.js',
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
    new WebpackOnBuildPlugin(function(stats) {
      const newlyCreatedAssets = stats.compilation.assets

      const unlinked = []
      fs.readdir(path.resolve(buildDir), (err, files) => {
        files.forEach(file => {
          if (!newlyCreatedAssets[file]) {
            console.log('Removed ', file)
            const del = path.resolve(buildDir + file)
            fs.stat(del, function(err, stat) {
              if (err == null) {
                try {
                  fs.unlink(path.resolve(buildDir + file))
                  console.log('Removed ', file)
                  unlinked.push(file)
                } catch (e) {}
              }
            })
          }
        })
        if (unlinked.length > 0) {
          console.log('Removed old assets: ', unlinked)
        }
      })
    }),
  ],
  postcss: require('./postcss'),
  target: 'web',
}

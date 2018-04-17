/* eslint-disable no-var */
const webpack = require('webpack')
const path = require('path')
const ExtractTextPlugin = require('extract-text-webpack-plugin')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const UglifyJsPlugin = require('uglifyjs-webpack-plugin')
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin')

const pkg = require('../package.json')
const dependencies = pkg.dependencies

const babelLoader = {
  loader: 'babel-loader',
  options: {
    cacheDirectory: false,
    presets: [['env', {modules: false}], 'react', 'stage-0'],
    plugins: ['transform-decorators-legacy'],
  },
}

const config = {
  node: {
    fs: 'empty',
    module: 'empty',
  },
  bail: true,
  devtool: false,
  entry: {
    app: path.resolve(__dirname, '..', 'src', 'index.tsx'),
    vendor: Object.keys(dependencies),
  },
  output: {
    publicPath: '/',
    path: path.resolve(__dirname, '../build'),
    filename: '[name].[chunkhash].js',
  },
  resolve: {
    alias: {
      app: path.resolve(__dirname, '..', 'app'),
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
        test: /\.ts(x?)$/,
        exclude: /node_modules/,
        loader: 'tslint-loader',
        enforce: 'pre',
        options: {
          emitErrors: true,
          configFile: path.resolve(__dirname, '..', 'tslint.json'),
        },
      },
      {
        test: /\.js$/,
        exclude: [/node_modules/, /(_s|S)pec\.js$/],
        use: 'eslint-loader',
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
        use: [{loader: 'thread-loader'}, babelLoader],
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
      VERSION: JSON.stringify(require('../package.json').version),
      'process.env.NODE_ENV': JSON.stringify('production'),
    }),
    new webpack.optimize.ModuleConcatenationPlugin(),
    new ForkTsCheckerWebpackPlugin({
      checkSyntacticErrors: true,
    }),
    new webpack.LoaderOptionsPlugin({
      postcss: require('./postcss'),
      options: {
        eslint: {
          failOnWarning: false,
          failOnError: false,
        },
      },
    }),
    new webpack.ProvidePlugin({
      $: 'jquery',
      jQuery: 'jquery',
    }),
    new ExtractTextPlugin('chronograf.css'),
    new UglifyJsPlugin({
      parallel: true,
      uglifyOptions: {
        ie8: false,
      },
    }),
    new webpack.optimize.CommonsChunkPlugin({
      name: 'vendor',
      minChunks(module) {
        return module.context && module.context.indexOf('node_modules') >= 0
      },
    }),
    new webpack.optimize.CommonsChunkPlugin({
      name: 'manifest',
    }),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, '..', 'src', 'index.template.html'),
      inject: 'body',
      chunksSortMode: 'dependency',
      favicon: 'assets/images/favicon.ico',
    }),
    function() {
      /* Webpack does not exit with non-zero status if error. */
      this.plugin('done', function(stats) {
        const {compilation: {errors}} = stats

        if (errors && errors.length) {
          errors.forEach(err => console.error(err))
          process.exit(1)
        }
      })
    },
  ],
  target: 'web',
}

module.exports = config

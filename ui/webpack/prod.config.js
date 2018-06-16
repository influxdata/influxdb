const webpack = require('webpack')
const path = require('path')
const get = require('lodash/get')
const MiniCssExtractPlugin = require('mini-css-extract-plugin')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const UglifyJsPlugin = require('uglifyjs-webpack-plugin')
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin')
const OptimizeCSSAssetsPlugin = require('optimize-css-assets-webpack-plugin')
const ProgressBarPlugin = require('progress-bar-webpack-plugin')

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
  mode: 'production',
  stats: 'errors-only',
  optimization: {
    concatenateModules: true,
    splitChunks: {
      name: 'vendor',
      minChunks: 2,
    },
    minimizer: [
      new UglifyJsPlugin({
        parallel: true,
        sourceMap: true,
        uglifyOptions: {
          ie8: false,
        },
      }),
      new OptimizeCSSAssetsPlugin({}),
    ],
  },
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
    rules: [
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
        test: /\.(sa|sc|c)ss$/,
        use: [
          MiniCssExtractPlugin.loader,
          'css-loader',
          'sass-loader',
          'resolve-url-loader',
          'sass-loader?sourceMap',
        ],
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
    }),
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
    new MiniCssExtractPlugin({
      filename: 'chronograf.[chunkhash].css',
      chunkFilename: '[id].css',
    }),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, '..', 'src', 'index.template.html'),
      inject: 'body',
      chunksSortMode: 'dependency',
      favicon: 'assets/images/favicon.ico',
    }),
    {
      apply: compiler => {
        compiler.hooks.afterEmit.tap('AfterEmitPlugin', ({compilation}) => {
          /* Webpack does not exit with non-zero status if error. */
          const errors = get(compilation, 'errors', [])

          if (errors.length) {
            errors.forEach(err => console.error(err))
            process.exit(1)
          }
        })
      },
    },
    new ProgressBarPlugin(),
  ],
  target: 'web',
}

module.exports = config

var webpack = require('webpack');
var path = require('path');
var ExtractTextPlugin = require("extract-text-webpack-plugin");
var HtmlWebpackPlugin = require("html-webpack-plugin");
var package = require('../package.json');
var dependencies = package.dependencies;

module.exports = {
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
        loader: ExtractTextPlugin.extract('style-loader', 'css-loader!sass-loader!resolve-url!sass?sourceMap'),
      },
      {
        test: /\.css$/,
        loader: ExtractTextPlugin.extract('style-loader', 'css-loader!postcss-loader'),
      },
      {
        test   : /\.(ico|png|cur|jpg|ttf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
        loader : 'file',
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
    includePaths: [path.resolve(__dirname, "node_modules")],
  },
  eslint: {
    failOnWarning: false,
    failOnError: false,
  },
  plugins: [
    new webpack.ProvidePlugin({
      $: "jquery",
      jQuery: "jquery",
    }),
    new ExtractTextPlugin("chronograf.css"),
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
  ],
  postcss: require('./postcss'),
  target: 'web',
};

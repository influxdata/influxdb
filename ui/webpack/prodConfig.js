/* eslint-disable no-var */
var webpack = require('webpack');
var path = require('path');
var ExtractTextPlugin = require("extract-text-webpack-plugin");
var HtmlWebpackPlugin = require("html-webpack-plugin");

var config = {
  devtool:  'hidden-source-map',
  entry: {
    app: path.resolve(__dirname, '..', 'src', 'index.js'),
  },
  output: {
    publicPath: '/build',
    path: path.resolve(__dirname, '../build'),
    filename: '[name].js',
  },
  resolve: {
    alias: {
      app: path.resolve(__dirname, '..', 'app'),
      src: path.resolve(__dirname, '..', 'src'),
      shared: path.resolve(__dirname, '..', 'src', 'shared'),
      style: path.resolve(__dirname, '..', 'src', 'style'),
      utils: path.resolve(__dirname, '..', 'src', 'utils'),
    },
  },
  module: {
    preLoaders: [
      {
        test: /\.js$/,
        exclude: [/node_modules/, /(_s|S)pec\.js$/],
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
        loader: ExtractTextPlugin.extract('style-loader', 'css-loader!sass-loader'),
      },
      {
        test: /\.css$/,
        loader: ExtractTextPlugin.extract('style-loader', 'css-loader!postcss-loader'),
      },
      {
        test   : /\.(ttf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
        loader : 'file-loader',
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'babel',
        query: {
          presets: ['es2015', 'react'],
          cacheDirectory: false, // Using the cache directory on production builds has never been helpful.
        },
      },
    ],
  },
  eslint: {
    failOnWarning: false,
    failOnError: false,
  },
  plugins: [
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify('production'),
      },
    }),
    new ExtractTextPlugin("style.css"),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, '..', 'src', 'index.template.html'),
      inject: 'body',
    }),
    new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false
      }
    }),
  ],
  postcss: require('./postcss'),
  target: 'web',
};

module.exports = config;

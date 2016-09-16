var webpack = require('webpack');
var path = require('path');
var ExtractTextPlugin = require("extract-text-webpack-plugin");
var HtmlWebpackPlugin = require("html-webpack-plugin");

module.exports = {
  devtool: 'eval-cheap-module-source-map',
  entry: {
    app: path.resolve(__dirname, '..', 'src', 'index.js'),
  },
  output: {
    publicPath: '/build',
    path: path.resolve(__dirname, '../build'),
    filename: '[name].dev.js',
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
        //loader: 'style!css!sass',
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
          cacheDirectory: true, // use a cache directory to speed up compilation
        },
      },
    ],
  },
  eslint: {
    failOnWarning: false,
    failOnError: false,
  },
  plugins: [
    // Fixes annoying, unhelpful warnings in Electron,
    // coming from reqwest's peer dependency on xhr2.
    //
    new webpack.IgnorePlugin(/xhr2/),
    new ExtractTextPlugin("style.css"),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, '..', 'src', 'index.template.html'),
      inject: 'body',
    }),
  ],
  postcss: require('./postcss'),
  target: 'web',
};

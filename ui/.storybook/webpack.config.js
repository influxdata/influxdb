const path = require('path');
var ExtractTextPlugin = require("extract-text-webpack-plugin");

// you can use this file to add your custom webpack plugins, loaders and anything you like.
// This is just the basic way to add addional webpack configurations.
// For more information refer the docs: https://getstorybook.io/docs/configurations/custom-webpack-config

// IMPORTANT
// When you add this file, we won't add the default configurations which is similar
// to "React Create App". This only has babel loader to load JavaScript.

module.exports = {
  debug: true,
  devtool: 'source-map',
  plugins: [
    new ExtractTextPlugin("style.css"),
  ],
  output: {
    publicPath: '/',
    path: path.resolve(__dirname, '../build'),
    filename: '[name].[chunkhash].dev.js',
  },
  module: {
    loaders: [
      {
        test: /\.scss$/,
        loader: ExtractTextPlugin.extract('style-loader', 'css-loader!sass-loader!resolve-url!sass?sourceMap'),
      },
      {
        test: /\.css$/,
        loader: ExtractTextPlugin.extract('style-loader', 'css-loader!postcss-loader'),
      },
      {
        test   : /\.(ico|png|jpg|ttf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
        options: {
          limit: 100000,
        },
        loader : 'file-loader',
      },
    ],
  },
  resolve: {
    alias: {
      src: path.resolve(__dirname, '..', 'src'),
      shared: path.resolve(__dirname, '..', 'src', 'shared'),
      style: path.resolve(__dirname, '..', 'src', 'style'),
      utils: path.resolve(__dirname, '..', 'src', 'utils'),
    },
  },
  sassLoader: {
    includePaths: [path.resolve(__dirname, "node_modules")],
  },
  postcss: require('../webpack/postcss'),
};

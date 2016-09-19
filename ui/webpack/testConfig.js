var path = require('path');
var hostname = 'localhost';
var port = 7357;

module.exports = {
  devtool: 'eval',
  entry: 'mocha!./spec/index.js',
  output: {
    filename: 'test.build.js',
    path: 'spec/',
    publicPath: 'http://' + hostname + ':' + port + '/spec'
  },
  module: {
    loaders: [
      {
        test: /\.js$/,
        exclude: /node_modules/,
        loader: 'babel-loader'
      },
      {
        test: /\.css/,
        exclude: /node_modules/,
        loader: 'style-loader!css-loader!postcss-loader',
      },
      {
        test: /\.scss/,
        exclude: /node_modules/,
        loader: 'style-loader!css-loader!sass-loader',
      },
      { // Sinon behaves weirdly with webpack, see https://github.com/webpack/webpack/issues/304
        test: /sinon\/pkg\/sinon\.js/,
        loader: 'imports?define=>false,require=>false',
      },
      {
        test: /\.json$/,
        loader: 'json',
      },
    ]
  },
  externals: {
    'react/addons': true,
    'react/lib/ExecutionEnvironment': true,
    'react/lib/ReactContext': true
  },
  devServer: {
    host: hostname,
    port: port,
  },
  resolve: {
    alias: {
      app: path.resolve(__dirname, '..', 'app'),
      src: path.resolve(__dirname, '..', 'src'),
      chronograf: path.resolve(__dirname, '..', 'src', 'chronograf'),
      shared: path.resolve(__dirname, '..', 'src', 'shared'),
      style: path.resolve(__dirname, '..', 'src', 'style'),
      utils: path.resolve(__dirname, '..', 'src', 'utils'),
      sinon: 'sinon/pkg/sinon',
    }
  }
};

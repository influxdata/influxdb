var webpack = require('webpack')
var path = require('path')

module.exports = function(config) {
  config.set({
    browsers: ['PhantomJS'],
    singleRun: true,
    frameworks: ['mocha', 'sinon-chai'],
    files: [
      'node_modules/babel-polyfill/dist/polyfill.js',
      'spec/spec-helper.js',
      'spec/index.js',
    ],
    preprocessors: {
      'spec/spec-helper.js': ['webpack', 'sourcemap'],
      'spec/index.js': ['webpack', 'sourcemap'],
    },
    reporters: ['dots'], // can also use 'verbose' and/or 'progress' for more detailed reporting
    webpack: {
      devtool: 'inline-source-map',
      module: {
        loaders: [
          {
            test: /\.js$/,
            exclude: /node_modules/,
            loader: 'babel-loader',
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
          {
            // Sinon behaves weirdly with webpack, see https://github.com/webpack/webpack/issues/304
            test: /sinon\/pkg\/sinon\.js/,
            loader: 'imports?define=>false,require=>false',
          },
          {
            test: /\.json$/,
            loader: 'json',
          },
        ],
      },
      externals: {
        'react/addons': true,
        'react/lib/ExecutionEnvironment': true,
        'react/lib/ReactContext': true,
      },
      resolve: {
        alias: {
          app: path.resolve(__dirname, 'app'),
          src: path.resolve(__dirname, 'src'),
          chronograf: path.resolve(__dirname, 'src', 'chronograf'),
          shared: path.resolve(__dirname, 'src', 'shared'),
          style: path.resolve(__dirname, 'src', 'style'),
          utils: path.resolve(__dirname, 'src', 'utils'),
          sinon: 'sinon/pkg/sinon',
        },
      },
    },
    webpackServer: {
      noInfo: true, // please don't spam the console when running in karma!
    },
  })
}

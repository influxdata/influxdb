const path = require('path')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const {CleanWebpackPlugin} = require('clean-webpack-plugin')
const webpack = require('webpack')
const {
  GIT_SHA,
  STATIC_DIRECTORY,
  BASE_PATH,
} = require('./src/utils/env')

module.exports = {
  context: __dirname,
  output: {
    path: path.resolve(__dirname, 'build'),
    publicPath: BASE_PATH,
    webassemblyModuleFilename: `${STATIC_DIRECTORY}[modulehash:10].wasm`,
    sourceMapFilename: `${STATIC_DIRECTORY}[name].js.map`,
  },
  entry: {
    app: './src/bootstrap.ts',
  },
  resolve: {
    alias: {
      src: path.resolve(__dirname, 'src'),
    },
    extensions: ['.tsx', '.ts', '.js', '.wasm'],
  },
  module: {
    rules: [
      {
        test: /\.wasm$/,
        type: 'webassembly/experimental',
      },
      {
        test: /\.tsx?$/,
        use: [
          {
            loader: 'ts-loader',
            options: {
              transpileOnly: true,
            },
          },
        ],
      },
      {
        test: /\.(png|svg|jpg|gif)$/,
        use: [{
          loader: 'file-loader',
          options: {
            name: `${STATIC_DIRECTORY}[contenthash:10].[ext]`
          }
        }],
      },
      {
        test: /\.(woff|woff2|eot|ttf|otf)$/,
        use: [{
          loader: 'file-loader',
          options: {
            name: `${STATIC_DIRECTORY}[contenthash:10].[ext]`
          }
        }],
      },
    ],
  },
  plugins: [
    new CleanWebpackPlugin(),
    new HtmlWebpackPlugin({
      filename: 'index.html',
      template: './assets/index.html',
      favicon: './assets/images/favicon.ico',
      inject: 'body',
      base: BASE_PATH.slice(0, -1),
    }),
    new webpack.ProgressPlugin(),
    new webpack.EnvironmentPlugin({...process.env, GIT_SHA}),
  ],
  stats: {
    colors: true,
    children: false,
    modules: false,
    version: false,
    assetsSort: '!size',
    warningsFilter: [/export .* was not found in/, /'.\/locale' in/],
    excludeAssets: [/\.(hot-update|woff|eot|ttf|svg|ico|png)/],
  },
  performance: {hints: false},
}

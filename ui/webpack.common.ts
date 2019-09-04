const path = require('path')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const {CleanWebpackPlugin} = require('clean-webpack-plugin')
const webpack = require('webpack')
const GIT_SHA = require('child_process')
  .execSync('git rev-parse HEAD')
  .toString()

module.exports = {
  context: __dirname,
  output: {
    path: path.resolve(__dirname, 'build'),
    sourceMapFilename: '[name].js.map',
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
        use: ['file-loader'],
      },
      {
        test: /\.(woff|woff2|eot|ttf|otf)$/,
        use: ['file-loader'],
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

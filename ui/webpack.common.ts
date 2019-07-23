const path = require('path')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const {CleanWebpackPlugin} = require('clean-webpack-plugin')
const MiniCssExtractPlugin = require('mini-css-extract-plugin')
const ForkTsCheckerWebpackPlugin = require('fork-ts-checker-webpack-plugin')
const webpack = require('webpack')
const GIT_SHA = require('child_process')
  .execSync('git rev-parse HEAD')
  .toString()

const devMode = process.env.NODE_ENV !== 'production'

module.exports = {
  context: __dirname,
  output: {
    filename: devMode ? '[name].bundle.js' : '[name].[hash].bundle.js',
    path: path.resolve(__dirname, 'build'),
  },
  entry: {
    app: './src/index.tsx',
  },
  resolve: {
    alias: {
      src: path.resolve(__dirname, 'src'),
    },
    extensions: ['.tsx', '.ts', '.js'],
  },
  module: {
    rules: [
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
        test: /\.s?css$/i,
        use: [
          MiniCssExtractPlugin.loader,
          'css-loader',
          {
            loader: 'sass-loader',
            options: {
              implementation: require('sass'),
              hmr: process.env.NODE_ENV === 'development',
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
    new MiniCssExtractPlugin({
      filename: devMode ? '[name].css' : '[name].[hash].css',
      chunkFilename: devMode ? '[id].css' : '[id].[hash].css',
    }),
    new ForkTsCheckerWebpackPlugin({
      eslint: !devMode,
      warningsFilter: /export * was not found in/,
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
    warningsFilter: /export .* was not found in/,
    excludeAssets: [/\.(hot-update|woff|eot|ttf|svg|ico|png)/],
  },
  performance: {hints: false},
}

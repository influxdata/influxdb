var path = require("path");
var HtmlWebpackPlugin = require('html-webpack-plugin');

module.exports = {
  entry: {
    app: ["./ui/js/main.js"]
  },
  output : { 
    path: path.resolve(__dirname, "dist"),
    publicPath: "/",
    filename: "bundle.js"
  },
  plugins: [
    new HtmlWebpackPlugin({
      hash: true,
      filename: 'index.html',
      template: path.resolve(__dirname, "ui", "templates", 'index.html')
    })
  ]
};

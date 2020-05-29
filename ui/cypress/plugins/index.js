// This function is called when a project is opened or re-opened (e.g. due to
// the project's config changing)
const wp = require('@cypress/webpack-preprocessor')

module.exports = on => {
  const options = {
    webpackOptions: require('../webpack.config.js')
  }

  on('file:preprocessor', wp(options))
}

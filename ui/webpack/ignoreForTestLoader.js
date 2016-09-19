/* eslint-disable */

// Used for test only: use /ignore as a path for ignored resources (fonts, images, etc.).
// (See testem.json for how /ignore is handled.)

const moduleSource = "module.exports = '/ignore';";

module.exports = function() {
  if (this.cacheable) {
    this.cacheable();
  }

  return moduleSource;
};

// Tells webpack not to bother with other loaders in this chain.
// See https://github.com/webpack/null-loader/blob/master/index.js
module.exports.pitch = function() {
  if (this.cacheable) {
    this.cacheable();
  }

  return moduleSource;
};

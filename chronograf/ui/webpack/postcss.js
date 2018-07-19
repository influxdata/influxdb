/* eslint-disable */

var precss = require('precss');
var autoprefixer = require('autoprefixer');
var calc = require('postcss-calc');
var consoleReporter = require('postcss-reporter');
var browserReporter = require('postcss-browser-reporter');

module.exports = function() {
  return {
    defaults: [precss, autoprefixer, calc, consoleReporter, browserReporter],
  };
};

var jsdom = require('jsdom')

var doc = jsdom.jsdom('<!doctype html><html><body></body></html>')

var win = doc.defaultView

global.document = doc
global.window = win

// From mocha-jsdom https://github.com/rstacruz/mocha-jsdom/blob/master/index.js#L80
function propagateToGlobal (window) {
  for (let key in window) {
    if (!window.hasOwnProperty(key)) continue
      if (key in global) continue

        global[key] = window[key]
  }
}

window.then = function(cb, done) {
  window.setTimeout(function() {
    cb();
    if (typeof done === 'function') {
      done();
    }
  }, 0);
};

propagateToGlobal(win)

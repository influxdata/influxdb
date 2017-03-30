window.then = function(cb, done) {
  window.setTimeout(function() {
    cb()
    if (typeof done === 'function') {
      done()
    }
  }, 0)
}

const chai = require('chai')
chai.use(require('sinon-chai'))

global.expect = chai.expect

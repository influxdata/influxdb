const context = require.context('./', true, /Spec\.js$/)
context.keys().forEach(context)
module.exports = context

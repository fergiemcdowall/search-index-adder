const util = require('util')
const Transform = require('stream').Transform

const LowCase = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.LowCase = LowCase
util.inherits(LowCase, Transform)
LowCase.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  for (var fieldName in doc.normalised) {
    if (fieldName === 'id') continue  // dont lowcase ID field
    doc.normalised[fieldName] = doc.normalised[fieldName].toLowerCase()
  }
  this.push(JSON.stringify(doc))
  return end()
}

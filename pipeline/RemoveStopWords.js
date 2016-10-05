const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const util = require('util')

const RemoveStopWords = function (options) {
  this.options = options || {}
  this.options.fieldOptions = this.options.fieldOptions || {}
  Transform.call(this, { objectMode: true })
}
exports.RemoveStopWords = RemoveStopWords
util.inherits(RemoveStopWords, Transform)
RemoveStopWords.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  for (var fieldName in doc.normalised) {
    var fieldOptions = _defaults(
      this.options.fieldOptions[fieldName] || {},  // TODO- this is wrong
      {
        stopwords: this.options.stopwords || []
      })
    // remove stopwords
    doc.normalised[fieldName] =
      doc.normalised[fieldName].filter(function (item) {
        return (fieldOptions.stopwords.indexOf(item) === -1)
      }).filter(function (i) {  // strip out empty elements
        return i
      })
  }
  this.push(JSON.stringify(doc))
  return end()
}

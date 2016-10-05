const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const util = require('util')

const RemoveStopWords = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.RemoveStopWords = RemoveStopWords
util.inherits(RemoveStopWords, Transform)
RemoveStopWords.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  for (fieldName in doc.normalised) {
    var fieldOptions = _defaults(
      this.options.fieldOptions[fieldName] || {},  // TODO- this is wrong
      {
        stopwords: this.options.stopwords, // A string.split() expression to tokenize raw field input
      })
    doc.normalised[fieldName] =
      doc.normalised[fieldName].filter(function (item) {
        return fieldOptions.stopwords.indexOf(item)
      })
  }
  this.push(JSON.stringify(doc))
  return end()
}

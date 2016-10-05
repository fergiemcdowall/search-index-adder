const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const util = require('util')

const FieldedSearch = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.FieldedSearch = FieldedSearch
util.inherits(FieldedSearch, Transform)
FieldedSearch.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  for (fieldName in doc.vector) {
    var fieldOptions = _defaults(
      this.options.fieldOptions[fieldName] || {},  // TODO- this is wrong
      {
        fieldedSearch: this.options.fieldedSearch // can this field be searched on?
      })
    if (!fieldOptions.fieldedSearch && fieldName !== '*') delete doc.vector[fieldName]
  }
  this.push(JSON.stringify(doc))
  return end()
}


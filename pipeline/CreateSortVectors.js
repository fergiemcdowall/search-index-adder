const tf = require('term-frequency')
const tv = require('term-vector')
const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const util = require('util')

// convert term-frequency vectors into object maps
const objectify = function (result, item) {
  result[item[0]] = item[1]
  return result
}

const CreateSortVectors = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.CreateSortVectors = CreateSortVectors
util.inherits(CreateSortVectors, Transform)
CreateSortVectors.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  for (fieldName in doc.vector) {
    var fieldOptions = _defaults(
      this.options.fieldOptions[fieldName] || {},  // TODO- this is wrong
      {
        sortable: this.options.sortable // Should this field be sortable
      })
    
    if (fieldOptions.sortable) {
      doc.vector[fieldName] = tf.getTermFrequency(
        doc.normalised[fieldName],
        { scheme: tf.selfNumeric }
      ).reduce(objectify, {})
    }

  }
  this.push(JSON.stringify(doc))
  return end()
}

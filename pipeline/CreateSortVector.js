const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const util = require('util')

const CreateSortVector = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.CreateSortVector = CreateSortVector
util.inherits(CreateSortVector, Transform)
CreateSortVector.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  for (fieldName in doc.vector) {
    var fieldOptions = _defaults(
      this.options.fieldOptions[fieldName] || {},  // TODO- this is wrong
      {
        sortable: this.options.sortable // Should this field be sortable
      })
    
    if (fieldOptions.sortable) {
      doc.vector[fieldName] = tf.getTermFrequency(
        tv.getVector(field), {
          scheme: tf.doubleNormalization0point5,
          weight: tf.selfNumeric
        }
      ).reduce(objectify, {})
    }

  }
  this.push(JSON.stringify(doc))
  return end()
}

const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const hash = require('object-hash')
const sw = require('stopword')
const tf = require('term-frequency')
const tv = require('term-vector')
const util = require('util')

const IngestDoc = function (options) {
  this.options = options

  Transform.call(this, { objectMode: true })
}

exports.IngestDoc = IngestDoc
util.inherits(IngestDoc, Transform)
IngestDoc.prototype._transform = function (doc, encoding, end) {
  var ingestedDoc = {
    id: String(doc.id || hash(doc)),
    vector: {},
    stored: {}
  }

  this.options.fieldOptions = this.options.fieldOptions || {}
  this.options.fieldOptions['id'] = {
    fieldName: 'id',
    searchable: true,
    storeable: true
  }

  // composite field
  ingestedDoc.vector['*'] = {}

  // go through all fields in doc
  for (var fieldName in doc) {
    var fieldOptions = _defaults(
      this.options.fieldOptions[fieldName] || {}, // TODO- this is wrong
      {
        fieldName: fieldName,
        searchable: this.options.searchable,
        separator: this.options.separator,
        storeable: this.options.storeable,
        stopwords: this.options.stopwords,
        nGramLength: this.options.nGramLength
      })

    var field = doc[fieldName]

    // if storeable, store this field
    if (fieldOptions.storeable) {
      ingestedDoc.stored[fieldName] = field
    }

    // if the input object is not a string: jsonify and split on JSON
    // characters
    if (Object.prototype.toString.call(field) !== '[object String]') {
      field = JSON.stringify(field).split(/[\[\],{}:\"]+/).join(' ')
    }

    field = String(field).toLowerCase()
      .split(fieldOptions.separator)
      .filter(function (item) {
        if (item) {
          return item
        }
      })

    // work out searchable fields
    if (fieldOptions.searchable) {
      ingestedDoc.vector[fieldName] = tf.getTermFrequency(
        tv.getVector(
          sw.removeStopwords(field, fieldOptions.stopwords),
          fieldOptions.nGramLength
        ),
        { scheme: tf.doubleNormalization0point5 }
      ).reduce(function (result, item) {
        result[item[0].join(' ')] = item[1]
        return result
      }, {})

      // wildcard
      ingestedDoc.vector[fieldName]['*'] = 1
      // composite field

      for (var token in ingestedDoc.vector[fieldName]) {
        ingestedDoc.vector['*'][token] =
          ingestedDoc.vector['*'][token] || ingestedDoc.vector[fieldName][token]
        ingestedDoc.vector['*'][token] =
          (ingestedDoc.vector['*'][token] + ingestedDoc.vector[fieldName][token]) / 2
      }
    }
    // cast id to string
    ingestedDoc.stored.id = String(ingestedDoc.stored.id)

    // work out sortable fields
    if (fieldOptions.sortable) {
      ingestedDoc.vector[fieldName] = tf.getTermFrequency(
        tv.getVector(
          String(field).toLowerCase().split(fieldOptions.separator)
        ),
        { scheme: tf.selfNumeric }
      ).reduce(function (result, item) {
        result[item[0].join(' ')] = item[1]
        return result
      }, {})
    }
  }

  this.push(JSON.stringify(ingestedDoc))
  return end()
}

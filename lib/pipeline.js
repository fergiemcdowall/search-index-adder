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
        fieldedSearch: this.options.fieldedSearch, // can search on this field individually
        searchable: this.options.searchable,      // included in the wildcard search ('*')
        separator: this.options.separator,        // A string.split() expression to tokenize raw field input
        storeable: this.options.storeable,        // Store a cache of this field in the index
        stopwords: this.options.stopwords,        // Words to be disgarded
        nGramLength: this.options.nGramLength,    // see https://www.npmjs.com/package/term-vector
        preserveCase: this.options.preserveCase   // Keep upper and lower case distingishable
      })

    var field = doc[fieldName]

    // if storeable, store this field
    if (fieldOptions.storeable) {
      ingestedDoc.stored[fieldName] = field
    }

    // if the input object is not a string: jsonify and split on JSON
    // characters
    if (Object.prototype.toString.call(field) !== '[object String]') {
      field = JSON.stringify(field).split(/[\[\],{}:"]+/).join(' ')
    }

    if (!fieldOptions.preserveCase) {
      field = String(field).toLowerCase()
    }

    field = String(field)
      .split(fieldOptions.separator)
      .filter(function (item) {
        if (item) {
          return item
        }
      })

    var vec
    // if fieldedSearch or searchable, work out a document vector based on term frequency
    if ((fieldOptions.fieldedSearch || fieldOptions.searchable) && !fieldOptions.sortable) {
      vec = tf.getTermFrequency(
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
      vec['*'] = 1
    // if sortable, work out a sortable document vector
    } else if (fieldOptions.sortable) {
      vec = tf.getTermFrequency(
        tv.getVector(
          String(field).toLowerCase().split(fieldOptions.separator)
        ),
        { scheme: tf.selfNumeric }
      ).reduce(function (result, item) {
        result[item[0].join(' ')] = item[1]
        return result
      }, {})
    }

    // is this field searchable?
    if (fieldOptions.fieldedSearch) {
      ingestedDoc.vector[fieldName] = vec
    }

    // is this part of the searchable composite field ('*')?
    if (fieldOptions.searchable) {
      for (var token in vec) {
        ingestedDoc.vector['*'][token] = ingestedDoc.vector['*'][token] || vec[token]
        ingestedDoc.vector['*'][token] = (ingestedDoc.vector['*'][token] + vec[token]) / 2
      }
    }

    // cast id to string
    ingestedDoc.stored.id = String(ingestedDoc.stored.id)
  }

  this.push(JSON.stringify(ingestedDoc))
  return end()
}

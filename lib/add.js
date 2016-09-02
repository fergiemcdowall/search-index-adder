const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const async = require('async')
const bunyan = require('bunyan')
const levelup = require('levelup')
const sep = '￮'
const sw = require('stopword')
const util = require('util')
const wildChar = '*'

// munge passed options into defaults options and return
exports.getOptions = function (givenOptions, callbacky) {
  givenOptions = givenOptions || {}
  async.parallel([
    function (callback) {
      var defaultOps = {}
      defaultOps.deletable = true
      defaultOps.fieldedSearch = true
      defaultOps.store = true
      defaultOps.indexPath = 'si'
      defaultOps.logLevel = 'error'
      defaultOps.nGramLength = 1
      defaultOps.nGramSeparator = ' '
      defaultOps.separator = /[\|' \.,\-|(\n)]+/
      defaultOps.stopwords = sw.en
      defaultOps.log = bunyan.createLogger({
        name: 'search-index',
        level: givenOptions.logLevel || defaultOps.logLevel
      })
      callback(null, defaultOps)
    },
    function (callback) {
      if (!givenOptions.indexes) {
        levelup(givenOptions.indexPath || 'si', {
          valueEncoding: 'json'
        }, function (err, db) {
          callback(err, db)
        })
      } else {
        callback(null, null)
      }
    }
  ], function (err, results) {
    var options = _defaults(givenOptions, results[0])
    if (results[1] != null) {
      options.indexes = results[1]
    }
    return callbacky(err, options)
  })
}

// // munge passed batch options into defaults and return
// var processBatchOptions = function (siOptions, batchOptions) {
//   var defaultFieldOptions = {
//     filter: false,
//     nGramLength: siOptions.nGramLength,
//     searchable: true,
//     weight: 0,
//     store: siOptions.store,
//     fieldedSearch: siOptions.fieldedSearch
//   }
//   var defaultBatchOptions = {
//     batchName: 'Batch at ' + new Date().toISOString(),
//     fieldOptions: siOptions.fieldOptions || defaultFieldOptions,
//     defaultFieldOptions: defaultFieldOptions
//   }
//   batchOptions = _defaults(batchOptions || {}, defaultBatchOptions)
//   batchOptions.filters = _map(_filter(batchOptions.fieldOptions, 'filter'), 'fieldName')
//   if (_find(batchOptions.fieldOptions, ['fieldName', wildChar]) === -1) {
//     batchOptions.fieldOptions.push(defaultFieldOptions(wildChar))
//   }
//   return batchOptions
// }

const IndexBatch = function (batchOptions, indexer) {
  this.indexer = indexer
  this.batchOptions = batchOptions
  this.batchOptions.fieldOptions = this.batchOptions.fieldOptions || []
  this.deltaIndex = {}
  Transform.call(this, { objectMode: true })
}
exports.IndexBatch = IndexBatch
util.inherits(IndexBatch, Transform)
IndexBatch.prototype._transform = function (ingestedDoc, encoding, end) {
  var that = this
  this.push('processing doc ' + ingestedDoc.id)
  this.deltaIndex['DOCUMENT-VECTOR' + sep + ingestedDoc.id + sep] = ingestedDoc.vector
  this.deltaIndex['DOCUMENT' + sep + ingestedDoc.id + sep] = ingestedDoc.stored
  // TODO deal with filters
  var filters = this.batchOptions.fieldOptions.filter(function (item) {
    if (item.filter) return true
  }).map(function (item) {
    return {
      fieldName: item.fieldName,
      values: Object.keys(ingestedDoc.vector[item.fieldName])
    }
  }).reduce(function (result, item) {
    item.values.forEach(function (value) {
      if (value === wildChar) return   // ignore wildcard
      result.push(item.fieldName + sep + value + sep)
    })
    return result
  }, [])
  filters.push(sep + sep) // general case for no filters

  for (var fieldName in ingestedDoc.vector) {
    for (var token in ingestedDoc.vector[fieldName]) {
      var vMagnitude = ingestedDoc.vector[fieldName][token]
      filters.forEach(function (filter) {
        var tfKeyName = 'TF' + sep + fieldName + sep + token + sep + filter
        var dfKeyName = 'DF' + sep + fieldName + sep + token + sep + filter
        that.deltaIndex[tfKeyName] = that.deltaIndex[tfKeyName] || []
        that.deltaIndex[tfKeyName].push([vMagnitude, ingestedDoc.id])
        that.deltaIndex[dfKeyName] = that.deltaIndex[dfKeyName] || []
        that.deltaIndex[dfKeyName].push(ingestedDoc.id)
      })
    }
    this.deltaIndex['DOCUMENT-VECTOR' + sep + ingestedDoc.id + sep + fieldName + sep] =
      ingestedDoc.vector[fieldName]
  }
  return end()
}
IndexBatch.prototype._flush = function (end) {
  var that = this
  // merge this index into main index
  var s = new Readable()
  for (var key in this.deltaIndex) {
    s.push(JSON.stringify({
      key: key,
      value: this.deltaIndex[key]
    }) + '\n')
  }
  s.push(null)
  s.pipe(JSONStream.parse())
    .pipe(this.indexer.dbWriteStream({merge: true}))
    .on('data', function (data) {
      console.log(data)
    })
    .on('end', function () {
      that.push('batch replicated')
      return end()
    })
}


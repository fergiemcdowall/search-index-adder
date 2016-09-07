const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const Transform = require('stream').Transform
const sep = '￮'
const util = require('util')
const wildChar = '*'

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
  this.indexer.deleter([ingestedDoc.id])
    .on('data', function (data) {
      // what to do here..?
    })
    .on('end', function () {
      that.indexer.options.log.info('processing doc ' + ingestedDoc.id)
      that.push('processing doc ' + ingestedDoc.id)
      that.deltaIndex['DOCUMENT' + sep + ingestedDoc.id + sep] = ingestedDoc.stored
      // TODO deal with filters
      var filters = that.batchOptions.fieldOptions.filter(function (item) {
        if (item.filter) return true
      }).map(function (item) {
        return {
          fieldName: item.fieldName,
          values: Object.keys(ingestedDoc.vector[item.fieldName])
        }
      }).reduce(function (result, item) {
        item.values.forEach(function (value) {
          if (value === wildChar) return // ignore wildcard
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
        that.deltaIndex['DOCUMENT-VECTOR' + sep + ingestedDoc.id + sep + fieldName + sep] =
          ingestedDoc.vector[fieldName]
      }
      return end()
    })
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

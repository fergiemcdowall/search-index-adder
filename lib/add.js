const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const Transform = require('stream').Transform
const sep = '￮'
const util = require('util')

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
  ingestedDoc = JSON.parse(ingestedDoc)
  this.indexer.deleter([ingestedDoc.id])
    .on('data', function (data) {
      // what to do here..?
    })
    .on('end', function () {
      that.indexer.options.log.info('processing doc ' + ingestedDoc.id)
      that.push('processing doc ' + ingestedDoc.id)
      that.deltaIndex['DOCUMENT' + sep + ingestedDoc.id + sep] = ingestedDoc.stored
      for (var fieldName in ingestedDoc.vector) {
        for (var token in ingestedDoc.vector[fieldName]) {
          var vMagnitude = ingestedDoc.vector[fieldName][token]
          var tfKeyName = 'TF' + sep + fieldName + sep + token + sep + sep + sep
          var dfKeyName = 'DF' + sep + fieldName + sep + token + sep + sep + sep
          that.deltaIndex[tfKeyName] = that.deltaIndex[tfKeyName] || []
          that.deltaIndex[tfKeyName].push([vMagnitude, ingestedDoc.id])
          that.deltaIndex[dfKeyName] = that.deltaIndex[dfKeyName] || []
          that.deltaIndex[dfKeyName].push(ingestedDoc.id)
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

    })
    .on('end', function () {
      that.push('batch replicated')
      return end()
    })
}

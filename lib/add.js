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
  // ingestedDoc = JSON.parse(ingestedDoc)
  this.indexer.deleter([ingestedDoc.id], function (err) {
    if (err) that.indexer.log.info(err)
    that.indexer.options.log.info('processing doc ' + ingestedDoc.id)
    // that.push('processing doc ' + ingestedDoc.id)
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
  // merge this index into main index
  for (var key in this.deltaIndex) {
    this.push({
      key: key,
      value: this.deltaIndex[key]
    })
  }
  return end()
}

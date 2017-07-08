const Transform = require('stream').Transform
const util = require('util')

const emitIndexKeys = function (s) {
  for (var key in s.deltaIndex) {
    s.push({
      key: key,
      value: s.deltaIndex[key]
    })
  }
  s.deltaIndex = {}
}

const noop = function (_, cb) { cb() }

const IndexBatch = function (deleter, options) {
  this.batchSize = options.batchSize || 1000
  this.deleter = deleter
  this.fastSort = options.fastSort
  this.appendOnly = options.appendOnly
  this.keySeparator = options.keySeparator || '￮'
  this.log = options.log
  this.storeDocument = options.storeDocument
  this.storeVector = options.storeVector
  this.deltaIndex = {}
  Transform.call(this, { objectMode: true })
}
exports.IndexBatch = IndexBatch
util.inherits(IndexBatch, Transform)
IndexBatch.prototype._transform = function (ingestedDoc, encoding, end) {
  const sep = this.keySeparator
  const that = this
  const maybedelete = this.appendOnly ? noop : this.deleter.bind(this.indexer)
  maybedelete([ingestedDoc.id], function (err) {
    if (err) that.indexer.log.info(err)
    that.log.info('processing doc ' + ingestedDoc.id)
    if (that.storeDocument === true) {
      that.deltaIndex['DOCUMENT' + sep + ingestedDoc.id + sep] = ingestedDoc.stored
    }
    for (var fieldName in ingestedDoc.vector) {
      that.deltaIndex['FIELD' + sep + fieldName] = fieldName
      for (var token in ingestedDoc.vector[fieldName]) {
        if (that.fastSort) {
          var vMagnitude = ingestedDoc.vector[fieldName][token]
          var tfKeyName = 'TF' + sep + fieldName + sep + token
          that.deltaIndex[tfKeyName] = that.deltaIndex[tfKeyName] || []
          that.deltaIndex[tfKeyName].push([vMagnitude, ingestedDoc.id])
        }
        var dfKeyName = 'DF' + sep + fieldName + sep + token
        that.deltaIndex[dfKeyName] = that.deltaIndex[dfKeyName] || []
        that.deltaIndex[dfKeyName].push(ingestedDoc.id)
      }
      if (that.storeVector) {
        that.deltaIndex['DOCUMENT-VECTOR' + sep + fieldName + sep + ingestedDoc.id + sep] =
          ingestedDoc.vector[fieldName]
      }
    }
    var totalKeys = Object.keys(that.deltaIndex).length
    if (totalKeys > that.batchSize) {
      that.push({totalKeys: totalKeys})
      that.log.info(
        'deltaIndex is ' + totalKeys + ' long, emitting')
      emitIndexKeys(that)
    }
    return end()
  })
}
IndexBatch.prototype._flush = function (end) {
  // merge this index into main index
  emitIndexKeys(this)
  return end()
}

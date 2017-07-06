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
  this.deleter = deleter
  this.deltaIndex = {}
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.IndexBatch = IndexBatch
util.inherits(IndexBatch, Transform)
IndexBatch.prototype._transform = function (ingestedDoc, encoding, end) {
  const sep = this.options.keySeparator
  var that = this
  const maybedelete = this.options.appendOnly ? noop : this.deleter.bind(this.indexer)
  maybedelete([ingestedDoc.id], function (err) {
    if (err) that.indexer.log.info(err)
    that.options.log.info('processing doc ' + ingestedDoc.id)
    if (that.options.storeDocument === true) {
      that.deltaIndex['DOCUMENT' + sep + ingestedDoc.id + sep] = ingestedDoc.stored
    }
    for (var fieldName in ingestedDoc.vector) {
      that.deltaIndex['FIELD' + sep + fieldName] = fieldName
      for (var token in ingestedDoc.vector[fieldName]) {
        var vMagnitude = ingestedDoc.vector[fieldName][token]
        var tfKeyName = 'TF' + sep + fieldName + sep + token
        var dfKeyName = 'DF' + sep + fieldName + sep + token
        that.deltaIndex[tfKeyName] = that.deltaIndex[tfKeyName] || []
        that.deltaIndex[tfKeyName].push([vMagnitude, ingestedDoc.id])
        that.deltaIndex[dfKeyName] = that.deltaIndex[dfKeyName] || []
        that.deltaIndex[dfKeyName].push(ingestedDoc.id)
      }
      that.deltaIndex['DOCUMENT-VECTOR' + sep + fieldName + sep + ingestedDoc.id + sep] =
        ingestedDoc.vector[fieldName]
    }
    // console.log(Object.keys(that.deltaIndex).length)
    // console.log(that.batchOptions.batchSize)
    var totalKeys = Object.keys(that.deltaIndex).length
    if (totalKeys > that.options.batchSize) {
      that.push({totalKeys: totalKeys})
      that.options.log.info(
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

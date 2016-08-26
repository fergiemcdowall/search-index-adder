const Transform = require('stream').Transform
const util = require('util')

const DBWriteMergeStream = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.DBWriteMergeStream = DBWriteMergeStream
util.inherits(DBWriteMergeStream, Transform)
DBWriteMergeStream.prototype._transform = function (data, encoding, end) {
  var that = this
  this.options.indexes.get(data.key, function (err, val) {
    if (err) {
      if (err.notFound) {
        // do something with this error
        err.notFound
        that.options.indexes.put(data.key, data.value, function (err) {
          // do something with this error
          err
          end()
        })
      }
    } else {
      var newVal
      if (data.key.substring(0, 3) === 'TF￮') {
        newVal = data.value.concat(val).sort(function (a, b) {
          return b[0] - a[0]
        })
      } else if (data.key.substring(0, 3) === 'DF￮') {
        newVal = data.value.concat(val).sort()
      } else if (data.key === 'DOCUMENT-COUNT') {
        newVal = (+val) + (+data.value)
      } else {
        newVal = data.value
      }
      that.options.indexes.put(data.key, newVal, function (err) {
        // do something with this err
        err
        end()
      })
    }
  })
}

const DBWriteCleanStream = function (options) {
  this.batchSize = 1000
  this.currentBatch = []
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(DBWriteCleanStream, Transform)
DBWriteCleanStream.prototype._transform = function (data, encoding, end) {
  var that = this
  this.currentBatch.push(data)
  if (this.currentBatch.length % this.batchSize === 0) {
    this.options.indexes.batch(this.currentBatch, function (err) {
      // TODO: some nice error handling if things go wrong
      err
      that.push('indexing batch')
      that.currentBatch = [] // reset batch
      end()
    })
  } else {
    end()
  }
}
DBWriteCleanStream.prototype._flush = function (end) {
  var that = this
  this.options.indexes.batch(this.currentBatch, function (err) {
    // TODO: some nice error handling if things go wrong
    err
    that.push('remaining data indexed')
    end()
  })
}
exports.DBWriteCleanStream = DBWriteCleanStream

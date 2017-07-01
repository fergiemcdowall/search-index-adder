// deletes all references to a document from the search index

const util = require('util')
const Transform = require('stream').Transform

// Remove all keys in DB
exports.flush = function (options, callback) {
  const sep = options.keySeparator
  var deleteOps = []
  options.indexes.createKeyStream({gte: '0', lte: sep})
    .on('data', function (data) {
      deleteOps.push({type: 'del', key: data})
    })
    .on('error', function (err) {
      options.log.error(err, ' failed to empty index')
      return callback(err)
    })
    .on('end', function () {
      options.indexes.batch(deleteOps, callback)
    })
}

const DocVector = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.DocVector = DocVector
util.inherits(DocVector, Transform)
DocVector.prototype._transform = function (docId, encoding, end) {
  docId = JSON.parse(docId) + ''
  const sep = this.options.keySeparator
  var that = this
  var docKey = 'DOCUMENT' + sep + docId + sep
  this.options.indexes.get(docKey, function (err, val) {
    if (err) return end()
    var docFields = Object.keys(val)
    docFields.push('*')  // may not be there if compositeField: false
    docFields = docFields.map(function (item) {
      return 'DOCUMENT-VECTOR' + sep + item + sep + docId + sep
    })
    var i = 0
    // get value from db and push it to the stream
    var pushValue = function (key) {
      if (key === undefined) {
        that.push({
          key: 'DOCUMENT' + sep + docId + sep,
          lastPass: true
        })
        return end()
      }
      that.options.indexes.get(key, function (err, val) {
        if (!err) {
          that.push({
            key: key,
            value: val
          })
        }
        pushValue(docFields[i++])
      })
    }
    pushValue(docFields[0])
  })
}

const DBEntries = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.DBEntries = DBEntries
util.inherits(DBEntries, Transform)
DBEntries.prototype._transform = function (vector, encoding, end) {
  if (vector.lastPass === true) {
    this.push({key: vector.key})
    return end()
  }
  const sep = this.options.keySeparator
  var that = this
  var field = vector.key.split(sep)[1]
  var docId = vector.key.split(sep)[2]
  var vectorKeys = Object.keys(vector.value)
  var i = 0
  var pushRecalibrationvalues = function (vec) {
    that.push({
      key: 'TF' + sep + field + sep + vectorKeys[i],
      value: docId
    })
    that.push({
      key: 'DF' + sep + field + sep + vectorKeys[i],
      value: docId
    })
    if (++i === vectorKeys.length) {
      that.push({key: vector.key})
      return end()
    } else {
      pushRecalibrationvalues(vector[vectorKeys[i]])
    }
  }
  pushRecalibrationvalues(vector[vectorKeys[0]])

  // TODO: fix this!
  // this.push({key: 'DOCUMENT-COUNT'})
}

const RecalibrateDB = function (options) {
  this.options = options
  this.batch = []
  Transform.call(this, { objectMode: true })
}
exports.RecalibrateDB = RecalibrateDB
util.inherits(RecalibrateDB, Transform)
// todo: handle deletion of wildcards, and do batched deletes
RecalibrateDB.prototype._transform = function (dbEntry, encoding, end) {
  const sep = this.options.keySeparator
  var that = this
  this.options.indexes.get(dbEntry.key, function (err, value) {
    if (err) {
      that.options.log.info(err)
    }
    // handle errors better
    if (!value) value = []
    var docId = dbEntry.value + ''
    var dbInstruction = {}
    dbInstruction.key = dbEntry.key
    if (dbEntry.key.substring(0, 3) === 'TF' + sep) {
      dbInstruction.value = value.filter(function (item) {
        return (item[1] !== docId)
      })
      if (dbInstruction.value.length === 0) {
        dbInstruction.type = 'del'
      } else {
        dbInstruction.type = 'put'
      }
      that.batch.push(dbInstruction)
      return end()
    } else if (dbEntry.key.substring(0, 3) === 'DF' + sep) {
      value.splice(value.indexOf(docId), 1)
      dbInstruction.value = value
      if (dbInstruction.value.length === 0) {
        dbInstruction.type = 'del'
      } else {
        dbInstruction.type = 'put'
      }
      that.batch.push(dbInstruction)
      return end()
    } else if (dbEntry.key.substring(0, 16) === 'DOCUMENT-VECTOR' + sep) {
      dbInstruction.type = 'del'
      that.batch.push(dbInstruction)
      return end()
    } else if (dbEntry.key.substring(0, 9) === 'DOCUMENT' + sep) {
      dbInstruction.type = 'del'
      that.batch.push(dbInstruction)
      // "DOCUMENT" should be the last instruction for the document
      that.options.indexes.batch(that.batch, function (err) {
        if (err) {
          // then what?
        }
        that.batch = []
        return end()
      })
    }
  })
}

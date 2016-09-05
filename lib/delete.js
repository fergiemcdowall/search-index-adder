// deletes all references to a document from the search index

var _flatten = require('lodash.flatten')
var _intersection = require('lodash.intersection')
var _remove = require('lodash.remove')
var _uniq = require('lodash.uniq')
var a = require('async')
const util = require('util')
const Transform = require('stream').Transform
const sep = '￮'

var deleteThisBatch = function (options, numberDocsToDelete, deleteBatch, results, callbacky) {
  results = _flatten(results).sort()
  results = _uniq(results)
  a.mapLimit(
    results,
    5,
    function (key, callback) {
      options.indexes.get(key, function (err, value) {
        var dbInstruction = {
          type: 'put',
          key: key
        }
        var recalibratedValue = []
        if (key.substring(0, 2) === 'DF') {
          recalibratedValue = _remove(value, function (n) {
            return (deleteBatch.indexOf(n + '') === -1)
          }).sort()
        } else if (key.substring(0, 2) === 'TF') {
          recalibratedValue = value.filter(function (item) {
            return (deleteBatch.indexOf(item[1]) === -1)
          }).sort(function (a, b) {
            return b[0] - a[0]
          })
        }
        if (recalibratedValue.length === 0) {
          dbInstruction.type = 'del'
        } else {
          dbInstruction.value = recalibratedValue
        }
        return callback(err, dbInstruction)
      })
    }, function (err, dbInstructions) {
      if (err) {
        options.log.info(err)
      }
      deleteBatch.forEach(function (docID) {
        dbInstructions.push({
          type: 'del',
          key: 'DELETE-DOCUMENT￮' + docID
        })
      })
      options.indexes.batch(dbInstructions, function (err) {
        if (err) {
          options.log.warn('Ooops!', err)
        } else {
          options.log.info({docids: deleteBatch}, 'DELETE')
        }
        // make undefined error null
        if (typeof err === undefined) err = null
        options.indexes.get('DOCUMENT-COUNT', function (err, value) {
          var docCount
          // no DOCUMENT-COUNT set- first indexing
          if (err) {
            docCount = 0
          } else {
            docCount = (+value - (numberDocsToDelete))
          }
          options.indexes.put('DOCUMENT-COUNT',
            docCount,
            function (errr) {
              return callbacky(errr)
            })
        })
      })
    })
}

// Try to delete the batch, throw errors if config, or formatting prevents deletion
exports.tryDeleteBatch = function (options, deleteBatch, callbacky) {
  var numberDocsToDelete = 0
  if (!Array.isArray(deleteBatch)) {
    deleteBatch = [deleteBatch]
  }
  a.series([
    function (callback) {
      if (!options.deletable) {
        options.indexes.get('DF￮*￮*￮￮', function (err, keys) {
          // are any of the deletebatch in the index?
          var keysInIndex = _intersection(deleteBatch, keys)
          var docExistsErr = null
          if (keysInIndex.length > 0) {
            docExistsErr = new Error(
              'this index is non-deleteable, and some of the documents you are deleting have IDs that are already present in the index. Either reinitialize index with "deletable: true" or alter the IDs of the new documents')
            return callbacky(docExistsErr)
          } else {
            return callback(err, null)
          }
        })
      } else {
        return callback(null, null)
      }
    },
    function (callback) {
      a.mapLimit(deleteBatch, 5, function (docID, mapCallback) {
        options.indexes.get('DELETE-DOCUMENT￮' + docID, function (err, keys) {
          if (err) {
            if (err.name === 'NotFoundError') {
              return mapCallback(null, [])
            }
          } else {
            numberDocsToDelete++
          }
          return mapCallback(err, keys)
        })
      }, function (err, results) {
        if (err) {
          return callbacky(err)
        } else {
          deleteThisBatch(options, numberDocsToDelete, deleteBatch, results, function (err) {
            return callbacky(err)
          })
        }
      })
    }
  ])
}

// Remove all keys in DB
exports.flush = function (options, callback) {
  var deleteOps = []
  options.indexes.createKeyStream({gte: '0', lte: '￮'})
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
  docId = JSON.parse(docId)
  var that = this
  this.options.indexes.createReadStream({
    gte: 'DOCUMENT-VECTOR' + sep + docId + sep,
    lte: 'DOCUMENT-VECTOR' + sep + docId + sep + sep
  }).on('data', function(data) {
    that.push(data)
  }).on('close', function() {
    return end()
  })
}

const DBEntries = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.DBEntries = DBEntries
util.inherits(DBEntries, Transform)
DBEntries.prototype._transform = function (vector, encoding, end) {
  var docId
  for (var k in vector.value) {
    docId = vector.key.split(sep)[1]
    var field = vector.key.split(sep)[2]
    this.push({
      key: 'TF' + sep + field + sep + k + sep + sep + sep,
      value: docId
    })
    this.push({
      key: 'DF' + sep + field + sep + k + sep + sep + sep,
      value: docId
    })
  }
  this.push({key: vector.key})
  this.push({key: 'DOCUMENT' + sep + docId + sep})

  // TODO: fix this!
  // this.push({key: 'DOCUMENT-COUNT'})
  return end()
}

const RecalibrateDB = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.RecalibrateDB = RecalibrateDB
util.inherits(RecalibrateDB, Transform)
RecalibrateDB.prototype._transform = function (dbEntry, encoding, end) {
  var that = this
  console.log(dbEntry.key)
  this.options.indexes.get(dbEntry.key, function(err, value) {
    if (err) console.log (err)
    // handle errors better
    var docId = dbEntry.value
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
    } else if (dbEntry.key.substring(0, 3) === 'DF' + sep) {
      value.splice(value.indexOf(docId), 1)
      dbInstruction.value = value
      if (dbInstruction.value.length === 0) {
        dbInstruction.type = 'del'
      } else {
        dbInstruction.type = 'put'
      }
    } else if (dbEntry.key.substring(0, 9) === 'DOCUMENT' + sep) {
      dbInstruction.type = 'del'
    } else if (dbEntry.key.substring(0, 16) === 'DOCUMENT-VECTOR' + sep) {
      dbInstruction.type = 'del'
    }
    that.options.indexes.batch([dbInstruction], function (err) {
      return end()
    })
  })
}

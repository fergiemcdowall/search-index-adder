// deletes all references to a document from the search index

var _defaults = require('lodash.defaults')
var _flatten = require('lodash.flatten')
var _intersection = require('lodash.intersection')
var _remove = require('lodash.remove')
var _uniq = require('lodash.uniq')
var a = require('async')

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
    function(callback) {
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
            return callback(null, null)
          }
        })
      } else {
        return callback(null, null)
      }
    },
    function(callback) {
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

// munge given options with default options
var getOptions = function (givenOptions, callbacky) {
  const async = require('async')
  const bunyan = require('bunyan')
  const levelup = require('levelup')
  const sw = require('stopword')
  givenOptions = givenOptions || {}
  async.parallel([
    function (callback) {
      var defaultOps = {}
      defaultOps.deletable = true
      defaultOps.fieldedSearch = true
      defaultOps.fieldsToStore = 'all'
      defaultOps.indexPath = 'si'
      defaultOps.logLevel = 'error'
      defaultOps.nGramLength = 1
      defaultOps.separator = /[\|' \.,\-|(\n)]+/
      defaultOps.stopwords = sw.getStopwords('en').sort()
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

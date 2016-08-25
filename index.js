const IndexBatch = require('./lib/addUtils.js').IndexBatch
const Readable = require('stream').Readable
const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const _isNumber = require('lodash.isnumber')
const _isPlainObject = require('lodash.isplainobject')
const _map = require('lodash.map')
const addUtils = require('./lib/addUtils.js')
const async = require('async')
const deleter = require('./lib/deleter.js')
const hash = require('object-hash')
const levelup = require('levelup')
const util = require('util')

module.exports = function (givenOptions, callback) {
  addUtils.getOptions(givenOptions, function (err, options) {
    var Indexer = {}
    Indexer.options = options
    Indexer.options.queue = addUtils.getQueue(Indexer)

    Indexer.deleteBatch = function (deleteBatch, APICallback) {
      deleter.tryDeleteBatch(options, deleteBatch, function (err) {
        return APICallback(Indexer.err)
      })
    }

    Indexer.flush = function (APICallback) {
      deleter.flush(options, function (err) {
        return APICallback(err)
      })
    }

    Indexer.close = function (callback) {
      options.indexes.close(function (err) {
        while (!options.indexes.isClosed()) {
          options.log.debug('closing...')
        }
        if (options.indexes.isClosed()) {
          options.log.debug('closed...')
          callback(err)
        }
      })
    }

    Indexer.add = function (batch, batchOptions, callback) {
      if (arguments.length === 2 && (typeof arguments[1] === 'function')) {
        callback = batchOptions
        batchOptions = undefined
      }
      addUtils.addBatchToIndex(Indexer.q,
        batch,
        batchOptions,
        options,
        callback)
    }

    Indexer.createWriteStream = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, {batchSize: 1000})
      return new IndexBatch(batchOptions, options)
    }

    //  return Indexer
    return callback(null, Indexer, Indexer.q)
  })
}



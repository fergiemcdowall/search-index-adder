// const IndexBatch = require('./lib/add.js').IndexBatch
const DBWriteCleanStream = require('./lib/replicate.js').DBWriteCleanStream
const DBWriteMergeStream = require('./lib/replicate.js').DBWriteMergeStream
const IngestDoc = require('./lib/pipelineStages/IngestDoc.js').IngestDoc
const IndexBatch = require('./lib/add').IndexBatch
const IndexBatch2 = require('./lib/add').IndexBatch2
const _defaults = require('lodash.defaults')
const add = require('./lib/add')
const deleter = require('./lib/delete.js')

module.exports = function (givenOptions, callback) {
  add.getOptions(givenOptions, function (err, options) {
    var Indexer = {}
    Indexer.options = options
    Indexer.options.queue = add.getQueue(Indexer)

    Indexer.deleteBatch = function (deleteBatch, APICallback) {
      deleter.tryDeleteBatch(options, deleteBatch, function (err) {
        return APICallback(err)
      })
    }

    Indexer.flush = function (APICallback) {
      deleter.flush(options, function (err) {
        return APICallback(err)
      })
    }

    Indexer.dbWriteStream = function (streamOps) {
      streamOps = _defaults(streamOps || {}, { merge: true })
      if (streamOps.merge === true) {
        return new DBWriteMergeStream(options)
      } else {
        return new DBWriteCleanStream(options)
      }
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
      // // so you batchOptions is optional
      // if (arguments.length === 2 && (typeof arguments[1] === 'function')) {
      //   callback = batchOptions
      //   batchOptions = undefined
      // }
      add.addBatchToIndex(
        Indexer.options.queue,
        batch,
        batchOptions,
        options,
        callback)
    }

    Indexer.createWriteStream2 = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, {batchSize: 1000})
      return new IndexBatch2(batchOptions, Indexer)
    }

    Indexer.createWriteStream = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, {batchSize: 1000})
      return new IndexBatch(batchOptions, Indexer)
    }

    Indexer.defaultPipeline = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, {batchSize: 1000})
      return new IngestDoc(batchOptions)
    }

    //  return Indexer
    return callback(err, Indexer)
  })
}

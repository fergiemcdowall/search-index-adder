const DBEntries = require('./lib/delete.js').DBEntries
const DocVector = require('./lib/delete.js').DocVector
const RecalibrateDB = require('./lib/delete.js').RecalibrateDB

const DBWriteCleanStream = require('./lib/replicate.js').DBWriteCleanStream
const DBWriteMergeStream = require('./lib/replicate.js').DBWriteMergeStream
const IngestDoc = require('./lib/pipeline.js').IngestDoc
const IndexBatch = require('./lib/add.js').IndexBatch
const _defaults = require('lodash.defaults')
const bunyan = require('bunyan')
const deleter = require('./lib/delete.js')
const levelup = require('levelup')
const sw = require('stopword')
const Readable = require('stream').Readable

module.exports = function (givenOptions, callback) {
  getOptions(givenOptions, function (err, options) {
    var Indexer = {}
    Indexer.options = options

    Indexer.deleteBatch = function (deleteBatch, APICallback) {
      deleter.tryDeleteDoc(options, deleteBatch, function (err) {
        return APICallback(err)
      })
    }

    Indexer.deleter = function (docIds) {
      const s = new Readable()
      docIds.forEach(function (docId) {
        s.push(JSON.stringify(docId))
      })
      s.push(null)
      return s.pipe(new DocVector(options))
        .pipe(new DBEntries(options))
        .pipe(new RecalibrateDB(options))
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

    Indexer.add = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, options)
      // this should probably not be instantiated on every call in
      // order to better deal with concurrent adds
      return new IndexBatch(batchOptions, Indexer)
    }

    Indexer.defaultPipeline = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, options)
      return new IngestDoc(batchOptions)
    }

    //  return Indexer
    return callback(err, Indexer)
  })
}

const getOptions = function (options, done) {
  options = _defaults(options, {
    deletable: true,
    batchSize: 1000,
    fieldedSearch: true,
    fieldOptions: {},
    preserveCase: false,
    storeable: true,
    searchable: true,
    indexPath: 'si',
    logLevel: 'error',
    nGramLength: 1,
    nGramSeparator: ' ',
    separator: /[\|' \.,\-|(\n)]+/,
    stopwords: sw.en,
    weight: 0
  })
  options.log = bunyan.createLogger({
    name: 'search-index',
    level: options.logLevel
  })
  if (!options.indexes) {
    levelup(options.indexPath || 'si', {
      valueEncoding: 'json'
    }, function (err, db) {
      options.indexes = db
      done(err, options)
    })
  } else {
    done(null, options)
  }
}

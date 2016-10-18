const DBEntries = require('./lib/delete.js').DBEntries
const DBWriteCleanStream = require('./lib/replicate.js').DBWriteCleanStream
const DBWriteMergeStream = require('./lib/replicate.js').DBWriteMergeStream
const DocVector = require('./lib/delete.js').DocVector
const IndexBatch = require('./lib/add.js').IndexBatch
const Readable = require('stream').Readable
const RecalibrateDB = require('./lib/delete.js').RecalibrateDB
const bunyan = require('bunyan')
const deleter = require('./lib/delete.js')
const docProc = require('docproc')
const leveldown = require('leveldown')
const levelup = require('levelup')
const pumpify = require('pumpify')

module.exports = function (givenOptions, callback) {
  getOptions(givenOptions, function (err, options) {
    var Indexer = {}
    Indexer.options = options

    Indexer.add = function (batchOptions) {
      batchOptions = Object.assign({}, options, batchOptions)
      // return new IndexBatch(batchOptions, Indexer)
      return pumpify.obj(
        new IndexBatch(batchOptions, Indexer),
        new DBWriteMergeStream(options)
      )
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

    Indexer.dbWriteStream = function (streamOps) {
      streamOps = Object.assign({}, { merge: true }, streamOps)
      if (streamOps.merge) {
        return new DBWriteMergeStream(options)
      } else {
        return new DBWriteCleanStream(options)
      }
    }

    Indexer.defaultPipeline = function (batchOptions) {
      batchOptions = Object.assign({}, options, batchOptions)
      return docProc.pipeline(batchOptions)
    }

    Indexer.deleteBatch = function (deleteBatch, done) {
      deleter.tryDeleteDoc(options, deleteBatch, function (err) {
        return done(err)
      })
    }

    Indexer.deleteStream = function (options) {
      return pumpify.obj(
        new DocVector(options),
        new DBEntries(options),
        new RecalibrateDB(options)
      )
    }

    Indexer.deleter = function (docIds) {
      const s = new Readable()
      docIds.forEach(function (docId) {
        s.push(JSON.stringify(docId))
      })
      s.push(null)
      return s.pipe(Indexer.deleteStream(options))
    }

    Indexer.flush = function (APICallback) {
      deleter.flush(options, function (err) {
        return APICallback(err)
      })
    }

    //  return Indexer
    return callback(err, Indexer)
  })
}

const getOptions = function (options, done) {
  options = Object.assign({}, {
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
    separator: /\\n|[\|' ><\.,\-|]+|\\u0003/,
    stopwords: [],
    weight: 0
  }, options)
  options.log = bunyan.createLogger({
    name: 'search-index',
    level: options.logLevel
  })
  if (!options.indexes) {
    levelup(options.indexPath || 'si', {
      valueEncoding: 'json',
      db: leveldown
    }, function (err, db) {
      options.indexes = db
      done(err, options)
    })
  } else {
    done(null, options)
  }
}

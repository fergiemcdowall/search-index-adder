const DBEntries = require('./lib/delete.js').DBEntries
const DocVector = require('./lib/delete.js').DocVector
const RecalibrateDB = require('./lib/delete.js').RecalibrateDB

const DBWriteCleanStream = require('./lib/replicate.js').DBWriteCleanStream
const DBWriteMergeStream = require('./lib/replicate.js').DBWriteMergeStream
// const IngestDoc = require('./lib/pipeline.js').IngestDoc


const IndexBatch = require('./lib/add.js').IndexBatch
const _defaults = require('lodash.defaults')
const bunyan = require('bunyan')
const deleter = require('./lib/delete.js')
const leveldown = require('leveldown')
const levelup = require('levelup')
const pumpify = require('pumpify')
const sw = require('stopword')
const Readable = require('stream').Readable


const pipeline = {}
pipeline.IngestDoc = require('./pipeline/IngestDoc.js').IngestDoc
pipeline.LowCase = require('./pipeline/LowCase.js').LowCase
pipeline.NormaliseFields = require('./pipeline/NormaliseFields.js').NormaliseFields
pipeline.Tokeniser = require('./pipeline/Tokeniser.js').Tokeniser
pipeline.RemoveStopWords = require('./pipeline/RemoveStopWords.js').RemoveStopWords
pipeline.CreateStoredDocument = require('./pipeline/CreateStoredDocument.js').CreateStoredDocument
pipeline.CreateCompositeVector = require('./pipeline/CreateCompositeVector.js').CreateCompositeVector
pipeline.CreateSortVectors = require('./pipeline/CreateSortVectors.js').CreateSortVectors
pipeline.CalculateTermFrequency = require('./pipeline/CalculateTermFrequency.js').CalculateTermFrequency
pipeline.FieldedSearch = require('./pipeline/FieldedSearch.js').FieldedSearch
pipeline.Spy = require('./pipeline/Spy.js').Spy



module.exports = function (givenOptions, callback) {
  getOptions(givenOptions, function (err, options) {
    var Indexer = {}
    Indexer.options = options

    Indexer.add = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, options)
      // this should probably not be instantiated on every call in
      // order to better deal with concurrent adds
      return new IndexBatch(batchOptions, Indexer)
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
      streamOps = _defaults(streamOps || {}, { merge: true })
      if (streamOps.merge === true) {
        return new DBWriteMergeStream(options)
      } else {
        return new DBWriteCleanStream(options)
      }
    }

    Indexer.defaultPipeline = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, options)
      return pumpify.obj(
        new pipeline.IngestDoc(batchOptions),
        new pipeline.CreateStoredDocument(batchOptions),
        new pipeline.NormaliseFields(batchOptions),
        new pipeline.LowCase(batchOptions),
        new pipeline.Tokeniser(batchOptions),
        new pipeline.RemoveStopWords(batchOptions),
        new pipeline.CalculateTermFrequency(batchOptions),
        new pipeline.CreateCompositeVector(batchOptions),
        new pipeline.CreateSortVectors(batchOptions),
        new pipeline.FieldedSearch(batchOptions)
      )
    }

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

    Indexer.pipeline = pipeline

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

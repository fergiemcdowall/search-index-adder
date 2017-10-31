/*
search-index-adder

The module that adds documents into search-index' can also be run as a
standalone

*/

const Logger = require('js-logger')
const levelup = require('levelup')
const API = require('./lib/API.js')

module.exports = function (givenOptions, callback) {
  getOptions(givenOptions, function (err, options) {
    const api = API(options)
    const Indexer = {}
    Indexer.options = options
    Indexer.add = api.add
    Indexer.concurrentAdd = api.concurrentAdd
    Indexer.concurrentDel = api.concurrentDel
    Indexer.close = api.close
    Indexer.dbWriteStream = api.dbWriteStream
    Indexer.defaultPipeline = api.defaultPipeline
    Indexer.deleteStream = api.deleteStream
    Indexer.deleter = api.deleter
    Indexer.feed = api.feed
    Indexer.flush = api.flush
    Indexer.synonyms = api.synonyms
    return callback(err, Indexer)
  })
}

const getOptions = function (options, done) {
  options = Object.assign({}, {
    appendOnly: false,
    deletable: true,
    batchSize: 1000,
    compositeField: true,
    fastSort: true,
    fieldedSearch: true,
    fieldOptions: {},
    preserveCase: false,
    keySeparator: '￮',
    storeable: true,
    storeDocument: true,
    storeVector: true,
    searchable: true,
    indexPath: 'si',
    logLevel: 'ERROR',
    logHandler: Logger.createDefaultHandler(),
    nGramLength: 1,
    nGramSeparator: ' ',
    separator: /\s|\\n|\\u0003|[-.,<>]/,
    stopwords: [],
    weight: 0,
    wildcard: true
  }, options)
  options.log = Logger.get('search-index-adder')
  // We pass the log level as string because the Logger[logLevel] returns
  // an object, and Object.assign deosn't make deep assign so it breakes
  // We used toUpperCase() for backward compatibility
  options.log.setLevel(Logger[options.logLevel.toUpperCase()])
  // Use the global one because the library doesn't support providing handler to named logger
  Logger.setHandler(options.logHandler)
  if (!options.indexes) {
    const leveldown = require('leveldown')
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

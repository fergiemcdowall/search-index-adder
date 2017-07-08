/*
search-index-adder

The module that adds documents into search-index' can also be run as a
standalone

*/

const bunyan = require('bunyan')
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
    logLevel: 'error',
    nGramLength: 1,
    nGramSeparator: ' ',
    separator: /\s|\\n|\\u0003|[-.,<>]/,
    stopwords: [],
    weight: 0,
    wildcard: true
  }, options)
  options.log = bunyan.createLogger({
    name: 'search-index',
    level: options.logLevel
  })
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

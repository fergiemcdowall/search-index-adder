const _defaults = require('lodash.defaults')
const _forEach = require('lodash.foreach')
const _filter = require('lodash.filter')
const _find = require('lodash.find')
const _flatten = require('lodash.flatten')
const _isEqual = require('lodash.isequal')
const _isNumber = require('lodash.isnumber')
const _isPlainObject = require('lodash.isplainobject')
const _isString = require('lodash.isstring')
const _last = require('lodash.last')
const _map = require('lodash.map')
const _reduce = require('lodash.reduce')
const _sortBy = require('lodash.sortby')
const async = require('async')
const bunyan = require('bunyan')
const hash = require('object-hash')
const levelup = require('levelup')
// const sep = '￮'
const Readable = require('stream').Readable
const sep = '￮'
const Transform = require('stream').Transform
const util = require('util')
const wildChar = '*'

const sid = require('search-index-deleter')
const tf = require('term-frequency')
const tv = require('term-vector')

module.exports = function (givenOptions, callback) {
  var Indexer = {}
  // initialize deleter and options
  async.series([
    function (callback) {
      getOptions(givenOptions, function (err, options) {
        Indexer.options = options
        return callback(err, options)
      })
    },
    function (callback) {
      sid(Indexer.options, function (err, deleter) {
        Indexer.deleter = deleter
        return callback(err, deleter)
      })
    }
  ], function (err, results) {
    if (err) {
      return callback(err, null)
    }

    // every batch is put into an internal queue, so that if callbacks
    // are not honoured when add-ing, index still adds batches
    // sequentially
    Indexer.q = async.queue(function (batch, callback) {
      batch.options = _defaults(batch.options, Indexer.options)
      // generate IDs if none are present and stringify numeric IDs
      var salt = 0
      batch.data.map(function (doc) {
        if (!doc.id) {
          doc.id = (++salt) + '-' + hash(doc)
        }
        doc.id = doc.id + '' // stringify ID
      })
      // before adding new docs, deleter checks the index to see if
      // documents with the same id exists and then deletes them
      Indexer.deleter.deleteBatch(_map(batch.data, 'id'), function (err) {
        // this needs to be changed to get 'deletable' to work properly
        if (err) {
          Indexer.options.log.debug(err)
          return callback(err)
        } else {
          // docs are now deleted if they existed, new docs can be added
          addBatch(batch.data, batch.options, Indexer.options, function (err) {
            return callback(err)
          })
        }
      })
    }, 1)

    // API call close()
    Indexer.close = function (callback) {
      Indexer.options.indexes.close(function (err) {
        while (!Indexer.options.indexes.isClosed()) {
          Indexer.options.log.debug('closing...')
        }
        if (Indexer.options.indexes.isClosed()) {
          Indexer.options.log.debug('closed...')
          callback(err)
        }
      })
    }

    // API call add()
    Indexer.add = function (batch, batchOptions, callback) {
      if (arguments.length === 2 && (typeof arguments[1] === 'function')) {
        callback = batchOptions
        batchOptions = undefined
      }
      addBatchToIndex(Indexer.q,
        batch,
        batchOptions,
        Indexer.options,
        callback)
    }

    Indexer.createWriteStream = function (batchOptions) {
      batchOptions = _defaults(batchOptions || {}, {batchSize: 1000})
      function IndexBatch (batchOptions) {
        this.batchSize = batchOptions.batchSize
        this.currentBatch = []
        Transform.call(this, { objectMode: true })
      }
      util.inherits(IndexBatch, Transform)
      IndexBatch.prototype._transform = function (data, encoding, end) {
        this.currentBatch.push(data)
        this.push('doc ' + data.id + ' added')
        var that = this
        if (this.currentBatch.length % this.batchSize == 0) {
          Indexer.add(this.currentBatch, {}, function(err) {
            // TODO: some nice error handling if things go wrong
            that.currentBatch = [] // reset batch
            that.push('batch indexed')
            end()
          })
        } else {
          end()
        }
      }
      IndexBatch.prototype._flush = function (end) {
        var that = this
        Indexer.add(this.currentBatch, {}, function(err) {
          that.push('remaining docs indexed')
          end()
        })
      }
      return new IndexBatch(batchOptions)
    }

    //  return Indexer
    return callback(null, Indexer)
  })
}


// Take a batch (array of documents to be indexed) and add it to the
// indexing queue. Batch options and indexing options are honoured
var addBatchToIndex = function (q, batch, batchOptions, indexerOptions, callback) {
  batchOptions = processBatchOptions(indexerOptions, batchOptions)
  if (!Array.isArray(batch) && _isPlainObject(batch)) {
    batch = [batch]
  }
  q.push({data: batch, options: batchOptions}, callback)
}

// Add this batch to the index respecting batch options and indexing
// options
var addBatch = function (batch, batchOptions, indexerOptions, callbackster) {
  var dbInstructions = []

  batch.forEach(function (doc) {
    // get database instructions for every doc. Instructions are keys
    // that must be added for every doc
    dbInstructions.push(getIndexEntries(doc, batchOptions, indexerOptions))
  })
  dbInstructions.push({
    type: 'put',
    key: 'DOCUMENT-COUNT',
    value: batch.length
  })

  // dbInstructions contains lots of duplicate keys. Reduce the array
  // so that all keys are unique
  dbInstructions = _flatten(dbInstructions)
  dbInstructions = _sortBy(dbInstructions, 'key')
  dbInstructions = _reduce(dbInstructions, function (prev, item) {
    if (item.key.substring(0, 6) === 'DELETE') {
      prev.push(item)
    } else if (item.key.substring(0, 8) === 'DOCUMENT') {
      prev.push(item)
    } else if (item.key.substring(0, 2) === 'TF') {
      if (item.key === _last(prev).key) {
        _last(prev).value.push(item.value[0])
      } else {
        prev.push(item)
      }
    } else if (item.key.substring(0, 2) === 'DF') {
      if (item.key === _last(prev).key) {
        _last(prev).value = _last(prev).value.concat(item.value)
      } else {
        prev.push(item)
      }
    }
    return prev
  }, [{key: '#', value: '#', type: '#'}])

  async.eachSeries(
    dbInstructions,
    function (item, callback) {
      indexerOptions.indexes.get(item.key, function (err, val) {
        if (err) indexerOptions.log.debug(err)
        if (item.key.substring(0, 2) === 'DF') {
          if (val) {
            item.value = item.value.concat(val)
          }
          item.value = item.value.sort()
        } else if (item.key.substring(0, 2) === 'TF') {
          if (val) {
            item.value = item.value.concat(val)
          }
          item.value = item.value.sort(function (a, b) {
            // sort buy score and then ID, descending:
            if (b[0] > a[0]) return 1
            if (b[0] < a[0]) return -1
            if (b[1] > a[1]) return 1
            if (b[1] < a[1]) return -1
            return 0
          })
        } else if (item.key === 'DOCUMENT-COUNT') {
          if (val) {
            item.value = +val + +(item.value)
          }
        }
        return callback(null)
      })
    },
    function (err) {
      if (err) indexerOptions.log.debug(err)
      dbInstructions.push({key: 'LAST-UPDATE-TIMESTAMP', value: Date.now()})
      indexerOptions.indexes.batch(dbInstructions, function (err) {
        if (err) {
          indexerOptions.log.info('Ooops!', err)
        } else {
          indexerOptions.log.info('BATCH ADDED')
        }
        return callbackster(null)
      })
    })
}

// get all index keys that this document will be added to
var getIndexEntries = function (doc, batchOptions, indexerOptions) {
  var docIndexEntries = []
  indexerOptions.log.info({docid: doc.id}, 'ADD')
  var docToStore = {}
  var freqsForComposite = [] // put document frequencies in here
  _forEach(doc, function (field, fieldName) {
    var fieldOptions = _defaults(_find(batchOptions.fieldOptions, ['fieldName', fieldName]) || {}, batchOptions.defaultFieldOptions)

    // console.log(fieldName)
    // console.log(fieldOptions.store)

    if (fieldName === 'id') {
      fieldOptions.stopwords = '' // because you cant run stopwords on id field
    } else {
      fieldOptions.stopwords = batchOptions.stopwords
    }

    // store the field BEFORE mutating.
    if (fieldOptions.store) docToStore[fieldName] = field

    // filter out invalid values from being indexes
    if (Array.isArray(field)) {
      // make filter fields searchable
      field = field.join(' ')
    } else if (field === null) {
      // skip null values
      delete doc[fieldName]
      indexerOptions.log.debug(doc.id + ': ' + fieldName + ' field is null, SKIPPING')
      // only index fields that are strings or numbers
    } else if (!(_isString(field) || _isNumber(field))) {
      // don't index unsearchable types
      delete doc[fieldName]
      indexerOptions.log.debug(doc.id + ': ' + fieldName +
        ' field not string or array, SKIPPING')
    }

    var vecOps = {
      separator: fieldOptions.separator || batchOptions.separator,
      stopwords: fieldOptions.stopwords || batchOptions.stopwords,
      nGramLength: fieldOptions.nGramLength || batchOptions.nGramLength
    }
    var v = tv.getVector(field + '', vecOps)
    var freq = tf.getTermFrequency(v, {
      scheme: 'doubleLogNormalization0.5',
      weight: fieldOptions.weight
    })
    freq.push([ [ wildChar ], 0 ]) // can do wildcard searh on this field
    if (fieldOptions.searchable) {
      freqsForComposite.push(freq)
    }
    if (fieldOptions.fieldedSearch) {
      freq.forEach(function (item) {
        var token = item[0].join(indexerOptions.nGramSeparator)
        getKeys(batchOptions, docIndexEntries, doc, token, item, fieldName)
        return
      })
    }
  })

  docIndexEntries.push({
    type: 'put',
    key: 'DOCUMENT' + sep + doc.id + sep,
    value: docToStore
  })

  freqsForComposite = _flatten(freqsForComposite).sort()
  freqsForComposite = _reduce(freqsForComposite, function (prev, item) {
    if (!prev[0]) {
      prev.push(item)
    } else if (_isEqual(item[0], _last(prev)[0])) {
      _last(prev)[1] = _last(prev)[1] + item[1]
    } else {
      prev.push(item)
    }
    return prev
  }, [])
  freqsForComposite = _forEach(freqsForComposite, function (item) {
    var token = item[0].join(indexerOptions.nGramSeparator)
    getKeys(batchOptions, docIndexEntries, doc, token, item, wildChar)
    return
  })

  if (indexerOptions.deletable) {
    docIndexEntries.push({
      type: 'put',
      key: 'DELETE-DOCUMENT' + sep + doc.id,
      value: _map(docIndexEntries, 'key')
    })
  }

  return docIndexEntries
}

var getKeys = function (batchOptions,
  docIndexEntries,
  doc,
  token,
  item,
  fieldName) {
  batchOptions.filters.forEach(function (filter) {
    // allow filtering on fields that are not formatted as Arrays
    if (!Array.isArray(doc[filter])) {
      doc[filter] = [doc[filter]]
    }
    _forEach(doc[filter], function (filterKey) {
      if ((filterKey !== 'undefined') && (filterKey !== undefined)) {
        docIndexEntries.push({
          type: 'put',
          key: 'DF' + sep + fieldName + sep + token + sep + filter + sep + filterKey,
          value: [doc.id]
        })
        docIndexEntries.push({
          type: 'put',
          key: 'TF' + sep + fieldName + sep + token + sep + filter + sep + filterKey,
          value: [[item[1].toFixed(16), doc.id]]
        })
      }
      return
    })
    return
  })
  docIndexEntries.push({
    type: 'put',
    key: 'DF' + sep + fieldName + sep + token + sep + sep,
    value: [doc.id]
  })
  docIndexEntries.push({
    type: 'put',
    key: 'TF' + sep + fieldName + sep + token + sep + sep,
    value: [[item[1].toFixed(16), doc.id]]
  })
}

// munge passed options into defaults options and return
var getOptions = function (givenOptions, callbacky) {
  givenOptions = givenOptions || {}
  async.parallel([
    function (callback) {
      var defaultOps = {}
      defaultOps.deletable = true
      defaultOps.fieldedSearch = true
      defaultOps.store = true
      defaultOps.indexPath = 'si'
      defaultOps.logLevel = 'error'
      defaultOps.nGramLength = 1
      defaultOps.nGramSeparator = ' '
      defaultOps.separator = /[\|' \.,\-|(\n)]+/
      defaultOps.stopwords = tv.getStopwords('en').sort()
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

// munge passed batch options into defaults and return
var processBatchOptions = function (siOptions, batchOptions) {
  var defaultFieldOptions = {
    filter: false,
    nGramLength: siOptions.nGramLength,
    searchable: true,
    weight: 0,
    store: siOptions.store,
    fieldedSearch: siOptions.fieldedSearch
  }
  var defaultBatchOptions = {
    batchName: 'Batch at ' + new Date().toISOString(),
    fieldOptions: siOptions.fieldOptions || defaultFieldOptions,
    defaultFieldOptions: defaultFieldOptions
  }
  batchOptions = _defaults(batchOptions || {}, defaultBatchOptions)
  batchOptions.filters = _map(_filter(batchOptions.fieldOptions, 'filter'), 'fieldName')
  if (_find(batchOptions.fieldOptions, ['fieldName', wildChar]) === -1) {
    batchOptions.fieldOptions.push(defaultFieldOptions(wildChar))
  }
  return batchOptions
}

const _ = require('lodash')
const async = require('async')
const hash = require('object-hash')
const sid = require('search-index-deleter')
const skeleton = require('log-skeleton')
const tf = require('term-frequency')
const tv = require('term-vector')

module.exports = function (options, callback) {
  init(options, function(err, Indexer) {
    if (err) return callback(err, null)
    // API : add
    Indexer.add = function (batch, batchOptions, callback) {
      if (arguments.length === 2 && _.isFunction(arguments[1])) {
        callback = batchOptions
        batchOptions = undefined
      }
      addBatchToIndex(batch,
                      batchOptions,
                      Indexer,
                      callback)
    }
    // API : close
    Indexer.close = function (callback) {
      close(this.options, callback)
    }
    // return new instance of search-index-indexer
    return callback(null, Indexer)
  })
}


var init = function (givenOptions, callback) {
  var Indexer = {}
  async.series([
    function(callback) {
      getOptions(givenOptions, function(err, options) {
        Indexer.options = options
        callback(err, options)
      })
    },
    function(callback) {
      sid(Indexer.options, callback)
    }    
  ], function (err, results) {
    if (err) callback(err, null)
    var deleter = results[1]
    Indexer.q = async.queue(function (batch, callback) {
      batch.options = _.defaults(batch.options, Indexer.options)
      // generate IDs if none are present and stringify numeric IDs
      var salt = 0
      batch.data.map(function (doc) {
        if (!doc.id) {
          doc.id = (++salt) + '-' + hash(doc)
        }
        doc.id = doc.id + '' // stringify ID
      })
      deleter.deleteBatch(_.map(batch.data, 'id'), function (err) {
        if (err) Indexer.options.log.info(err)
        addBatch(batch.data, batch.options, Indexer, function (err) {
          return callback(err)
        })
      })
    }, 1)
    return callback(err, Indexer)
  })
}

var getOptions = function (givenOptions, callbacky) {
  const _ = require('lodash')
  const bunyan = require('bunyan')
  var levelup = require('levelup')
  const tv = require('term-vector')
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
    var options = _.defaults(givenOptions, results[0])
    if (results[1] != null) {
      //      options = _.defaults(options, results[1])
      options.indexes = results[1]
    }
    return callbacky(err, options)
  })
}

var processBatchOptions = function (siOptions, batchOptions) {
  var defaultFieldOptions = {
    filter: false,
    nGramLength: siOptions.nGramLength,
    searchable: true,
    weight: 0,
    fieldedSearch: siOptions.fieldedSearch
  }
  var defaultBatchOptions = {
    batchName: 'Batch at ' + new Date().toISOString(),
    fieldOptions: siOptions.fieldOptions || defaultFieldOptions,
    fieldsToStore: siOptions.fieldsToStore,
    defaultFieldOptions: defaultFieldOptions
  }
  batchOptions = _.defaults(batchOptions || {}, defaultBatchOptions)
  batchOptions.filters = _.map(_.filter(batchOptions.fieldOptions, 'filter'), 'fieldName')
  if (_.find(batchOptions.fieldOptions, ['fieldName', '*']) === -1) {
    batchOptions.fieldOptions.push(defaultFieldOptions('*'))
  }
  return batchOptions
}

var close = function(options, callback) {
  options.indexes.close(function (err) {
    while (!options.indexes.isClosed()) {
      options.log.info('closing...')
    }
    if (options.indexes.isClosed()) {
      options.log.info('closed...')
      callback(err)
    }
  })      
}

var addBatchToIndex = function (batch, batchOptions, Indexer, callback) {
  batchOptions = processBatchOptions(Indexer.options, batchOptions)
  if (!_.isArray(batch) && _.isPlainObject(batch)) {
    batch = [batch]
  }
  Indexer.q.push({data: batch, options: batchOptions}, callback)
}

var removeInvalidFields = function (doc, log) {
  for (var fieldKey in doc) {
    if (_.isArray(doc[fieldKey])) continue
    else if (doc[fieldKey] === null) {
      delete doc[fieldKey]
      log.info(doc.id + ': ' + fieldKey + ' field is null, SKIPPING')
      // only index fields that are strings or numbers
    } else if (!(_.isString(doc[fieldKey]) || _.isNumber(doc[fieldKey]))) {
      delete doc[fieldKey]
      log.info(doc.id + ': ' + fieldKey +
               ' field not string or array, SKIPPING')
    }
  }
  return doc
}

var getIndexEntries = function (doc, batchOptions, Indexer) {
  var docIndexEntries = []
  if (!_.isPlainObject(doc)) {
    return callback(new Error('Malformed document'), {})
  }
  doc = removeInvalidFields(doc, Indexer.options.log)
  if (batchOptions.fieldsToStore === 'all') {
    batchOptions.fieldsToStore = Object.keys(doc)
  }
  Indexer.options.log.info('indexing ' + doc.id)
  docIndexEntries.push({
    type: 'put',
    key: 'DOCUMENT￮' + doc.id + '￮',
    value: _.pick(doc, batchOptions.fieldsToStore)
  })
  var freqsForComposite = [] // put document frequencies in here
  _.forEach(doc, function (field, fieldName) {
    var fieldOptions = _.defaults(_.find(batchOptions.fieldOptions, ['fieldName', fieldName]) || {}, batchOptions.defaultFieldOptions)
    if (fieldName === 'id') {
      fieldOptions.stopwords = '' // because you cant run stopwords on id field
    } else {
      fieldOptions.stopwords = batchOptions.stopwords
    }
    if (_.isArray(field)) field = field.join(' ') // make filter fields searchable
    var vecOps = {
      separator: fieldOptions.separator || batchOptions.separator,
      stopwords: fieldOptions.stopwords || batchOptions.stopwords,
      nGramLength: fieldOptions.nGramLength || batchOptions.nGramLength
    }
    var v = tv.getVector(field + '', vecOps)
    v.push([['*'], 1]) // can do wildcard searh on this field
    debugger;
    var freq = tf.getTermFrequency(v, {
      scheme: 'doubleLogNormalization0.5',
      weight: fieldOptions.weight
    })
    if (fieldOptions.searchable) {
      freqsForComposite.push(freq)
    }
    if (fieldOptions.fieldedSearch) {
      freq.forEach(function (item) {
        docIndexEntries = docIndexEntries.concat(
          fieldIndexEntries(fieldName, item, Indexer, batchOptions, doc))
      })
    }
  })
  // generate * field
  _(freqsForComposite)
    .flatten()
    .sort()
    .reduce(function (prev, item) {
      if (!prev[0]) {
        prev.push(item)
      } else if (_.isEqual(item[0], _.last(prev)[0])) {
        _.last(prev)[1] = _.last(prev)[1] + item[1]
      } else {
        prev.push(item)
      }
      return prev
    }, [])
    .forEach(function (item) {
      docIndexEntries = docIndexEntries.concat(
        fieldIndexEntries('*', item, Indexer, batchOptions, doc))
    })
  docIndexEntries.push({
    type: 'put',
    key: 'DELETE-DOCUMENT￮' + doc.id,
    value: _.map(docIndexEntries, 'key')
  })
  return docIndexEntries
}


var fieldIndexEntries = function (fieldName, item, Indexer, batchOptions, doc) {
  var entries = []
  var token = item[0].join(Indexer.options.nGramSeparator)
  batchOptions.filters.forEach(function (filter) {
    _.forEach(doc[filter], function (filterKey) {
      entries.push({
        type: 'put',
        key: 'TF￮' + fieldName + '￮' + token + '￮' + filter + '￮' + filterKey,
        value: [doc.id]
      })
      entries.push({
        type: 'put',
        key: 'RI￮' + fieldName + '￮' + token + '￮' + filter + '￮' + filterKey,
        value: [[item[1].toFixed(16), doc.id]]
      })
    })
  })
  entries.push({
    type: 'put',
    key: 'TF￮' + fieldName + '￮' + token + '￮￮',
    value: [doc.id]
  })
  entries.push({
    type: 'put',
    key: 'RI￮' + fieldName + '￮' + token + '￮￮',
    value: [[item[1].toFixed(16), doc.id]]
  })
  return entries
}

var addBatch = function (batch, batchOptions, Indexer, callbackster) {
  var dbInstructions = []
  batch.forEach(function (doc) {
    dbInstructions.push(getIndexEntries(doc, batchOptions, Indexer))
  })
  dbInstructions.push({
    type: 'put',
    key: 'DOCUMENT-COUNT',
    value: batch.length
  })
  dbInstructions = _(dbInstructions)
    .flatten()
    .sortBy('key')
    .reduce(function (prev, item) {
      if (item.key.substring(0, 6) === 'DELETE') {
        prev.push(item)
      } else if (item.key.substring(0, 8) === 'DOCUMENT') {
        prev.push(item)
      } else if (item.key.substring(0, 2) === 'RI') {
        if (item.key === _.last(prev).key) {
          _.last(prev).value.push(item.value[0])
        } else {
          prev.push(item)
        }
      } else if (item.key.substring(0, 2) === 'TF') {
        if (item.key === _.last(prev).key) {
          _.last(prev).value = _.last(prev).value.concat(item.value)
        } else {
          prev.push(item)
        }
      }
      return prev
    }, [])
  async.eachSeries(
    dbInstructions,
    function (item, callback) {
      Indexer.options.indexes.get(item.key, function (err, val) {
        if (err) Indexer.options.log.info(err)
        if (item.key.substring(0, 2) === 'TF') {
          if (val) {
            item.value = item.value.concat(val)
          }
          item.value = item.value.sort()
        } else if (item.key.substring(0, 2) === 'RI') {
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
          // console.log(val)
          if (val) {
            item.value = +val + +(item.value)
          }
        }
        return callback(null)
      })
    },
    function (err) {
      if (err) Indexer.options.log.info(err)
      dbInstructions.push({key: 'LAST-UPDATE-TIMESTAMP', value: Date.now()})
      Indexer.options.indexes.batch(dbInstructions, function (err) {
        if (err) Indexer.options.log.info('Ooops!', err)
        else Indexer.options.log.info('batch indexed!')
        return callbackster(null)
      })
    })
}

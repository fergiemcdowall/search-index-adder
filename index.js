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
const _pick = require('lodash.pick')
const _reduce = require('lodash.reduce')
const _sortBy = require('lodash.sortby')
const async = require('async')
const bunyan = require('bunyan')
const hash = require('object-hash')
const levelup = require('levelup')
const sid = require('search-index-deleter')
const skeleton = require('log-skeleton')
const tf = require('term-frequency')
const tv = require('term-vector')

module.exports = function (givenOptions, callback) {
  var Indexer = {}
  getOptions(givenOptions, function (err, options) {
    if (err) callback(err, null)

    Indexer.close = function (callback) {
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

    Indexer.options = options

    sid(Indexer.options, function (err, deleter) {
      if (err) {
        callback(err, null)
      }
      var log = skeleton((Indexer.options) ? Indexer.options.log : undefined)
      var q = async.queue(function (batch, callback) {
        batch.options = _defaults(batch.options, options)
        // generate IDs if none are present and stringify numeric IDs
        var salt = 0
        batch.data.map(function (doc) {
          if (!doc.id) {
            doc.id = (++salt) + '-' + hash(doc)
          }
          doc.id = doc.id + '' // stringify ID
        })
        deleter.deleteBatch(_map(batch.data, 'id'), function (err) {
          if (err) log.info(err)
          addBatch(batch.data, batch.options, function (err) {
            return callback(err)
          })
        })
      }, 1)

      Indexer.add = function (batch, batchOptions, callback) {
        if (arguments.length === 2 && (typeof arguments[1] === 'function')) {
          callback = batchOptions
          batchOptions = undefined
        }
        addBatchToIndex(batch,
          batchOptions,
          callback)
      }

      Indexer.getOptions = function () {
        return Indexer.options
      }

      var addBatchToIndex = function (batch, batchOptions, callback) {
        batchOptions = processBatchOptions(Indexer.options, batchOptions)
        if (!Array.isArray(batch) && _isPlainObject(batch)) {
          batch = [batch]
        }
        q.push({data: batch, options: batchOptions}, callback)
      }

      var removeInvalidFields = function (doc) {
        for (var fieldKey in doc) {
          if (Array.isArray(doc[fieldKey])) continue
          else if (doc[fieldKey] === null) {
            delete doc[fieldKey]
            log.info(doc.id + ': ' + fieldKey + ' field is null, SKIPPING')
            // only index fields that are strings or numbers
          } else if (!(_isString(doc[fieldKey]) || _isNumber(doc[fieldKey]))) {
            delete doc[fieldKey]
            log.info(doc.id + ': ' + fieldKey +
                     ' field not string or array, SKIPPING')
          }
        }
        return doc
      }

      function getIndexEntries (doc, batchOptions) {
        var docIndexEntries = []
        if (!_isPlainObject(doc)) {
          return callback(new Error('Malformed document'), {})
        }
        doc = removeInvalidFields(doc)
        if (batchOptions.fieldsToStore === 'all') {
          batchOptions.fieldsToStore = Object.keys(doc)
        }
        log.info('indexing ' + doc.id)
        docIndexEntries.push({
          type: 'put',
          key: 'DOCUMENT￮' + doc.id + '￮',
          value: _pick(doc, batchOptions.fieldsToStore)
        })
        var freqsForComposite = [] // put document frequencies in here
        _forEach(doc, function (field, fieldName) {
          var fieldOptions = _defaults(_find(batchOptions.fieldOptions, ['fieldName', fieldName]) || {}, batchOptions.defaultFieldOptions)
          if (fieldName === 'id') {
            fieldOptions.stopwords = '' // because you cant run stopwords on id field
          } else {
            fieldOptions.stopwords = batchOptions.stopwords
          }
          if (Array.isArray(field)) field = field.join(' ') // make filter fields searchable

          var vecOps = {
            separator: fieldOptions.separator || batchOptions.separator,
            stopwords: fieldOptions.stopwords || batchOptions.stopwords,
            nGramLength: fieldOptions.nGramLength || batchOptions.nGramLength
          }
          var v = tv.getVector(field + '', vecOps)
//          v.push([['*'], 1]) // can do wildcard searh on this field
          var freq = tf.getTermFrequency(v, {
            scheme: 'doubleLogNormalization0.5',
            weight: fieldOptions.weight
          })
          freq.push([ [ '*' ], 0 ] )  // can do wildcard searh on this field
          if (fieldOptions.searchable) {
            freqsForComposite.push(freq)
          }
          if (fieldOptions.fieldedSearch) {
            freq.forEach(function (item) {
              var token = item[0].join(options.nGramSeparator)
              batchOptions.filters.forEach(function (filter) {
                _forEach(doc[filter], function (filterKey) {
                  docIndexEntries.push({
                    type: 'put',
                    key: 'TF￮' + fieldName + '￮' + token + '￮' + filter + '￮' + filterKey,
                    value: [doc.id]
                  })
                  docIndexEntries.push({
                    type: 'put',
                    key: 'RI￮' + fieldName + '￮' + token + '￮' + filter + '￮' + filterKey,
                    value: [[item[1].toFixed(16), doc.id]]
                  })
                })
              })
              docIndexEntries.push({
                type: 'put',
                key: 'TF￮' + fieldName + '￮' + token + '￮￮',
                value: [doc.id]
              })
              docIndexEntries.push({
                type: 'put',
                key: 'RI￮' + fieldName + '￮' + token + '￮￮',
                value: [[item[1].toFixed(16), doc.id]]
              })
            })
          }
        })
        // generate * field
        
        // _(freqsForComposite)
        //   .flatten()
        //   .sort()
        //   .reduce(function (prev, item) {
        //     if (!prev[0]) {
        //       prev.push(item)
        //     } else if (_isEqual(item[0], _last(prev)[0])) {
        //       _last(prev)[1] = _last(prev)[1] + item[1]
        //     } else {
        //       prev.push(item)
        //     }
        //     return prev
        //   }, [])
          // .forEach(function (item) {
          //   var token = item[0].join(options.nGramSeparator)
          //   batchOptions.filters.forEach(function (filter) {
          //     _forEach(doc[filter], function (filterKey) {
          //       docIndexEntries.push({
          //         type: 'put',
          //         key: 'TF￮*￮' + token + '￮' + filter + '￮' + filterKey,
          //         value: [doc.id]
          //       })
          //       docIndexEntries.push({
          //         type: 'put',
          //         key: 'RI￮*￮' + token + '￮' + filter + '￮' + filterKey,
          //         value: [[item[1].toFixed(16), doc.id]]
          //       })
          //     })
          //   })
          //   docIndexEntries.push({
          //     type: 'put',
          //     key: 'TF￮*￮' + token + '￮￮',
          //     value: [doc.id]
          //   })
          //   docIndexEntries.push({
          //     type: 'put',
          //     key: 'RI￮*￮' + token + '￮￮',
          //     value: [[item[1].toFixed(16), doc.id]]
          //   })
          // })


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
          var token = item[0].join(options.nGramSeparator)
          batchOptions.filters.forEach(function (filter) {
            _forEach(doc[filter], function (filterKey) {
              docIndexEntries.push({
                type: 'put',
                key: 'TF￮*￮' + token + '￮' + filter + '￮' + filterKey,
                value: [doc.id]
              })
              docIndexEntries.push({
                type: 'put',
                key: 'RI￮*￮' + token + '￮' + filter + '￮' + filterKey,
                value: [[item[1].toFixed(16), doc.id]]
              })
            })
          })
          docIndexEntries.push({
            type: 'put',
            key: 'TF￮*￮' + token + '￮￮',
            value: [doc.id]
          })
          docIndexEntries.push({
            type: 'put',
            key: 'RI￮*￮' + token + '￮￮',
            value: [[item[1].toFixed(16), doc.id]]
          })
        })
        docIndexEntries.push({
          type: 'put',
          key: 'DELETE-DOCUMENT￮' + doc.id,
          value: _map(docIndexEntries, 'key')
        })
        return docIndexEntries
      }

      function addBatch (batch, batchOptions, callbackster) {
        var dbInstructions = []
        batch.forEach(function (doc) {
          dbInstructions.push(getIndexEntries(doc, batchOptions))
        })
        dbInstructions.push({
          type: 'put',
          key: 'DOCUMENT-COUNT',
          value: batch.length
        })

        dbInstructions = _flatten(dbInstructions)
        dbInstructions = _sortBy(dbInstructions, 'key')
        dbInstructions = _reduce(dbInstructions, function (prev, item) {
          if (item.key.substring(0, 6) === 'DELETE') {
            prev.push(item)
          } else if (item.key.substring(0, 8) === 'DOCUMENT') {
            prev.push(item)
          } else if (item.key.substring(0, 2) === 'RI') {
            if (item.key === _last(prev).key) {
              _last(prev).value.push(item.value[0])
            } else {
              prev.push(item)
            }
          } else if (item.key.substring(0, 2) === 'TF') {
            if (item.key === _last(prev).key) {
              _last(prev).value = _last(prev).value.concat(item.value)
            } else {
              prev.push(item)
            }
          }
          return prev
        }, [])

        // dbInstructions = _(dbInstructions)
        //   .flatten()
        //   .sortBy('key')
        //   .reduce(function (prev, item) {
        //     if (item.key.substring(0, 6) === 'DELETE') {
        //       prev.push(item)
        //     } else if (item.key.substring(0, 8) === 'DOCUMENT') {
        //       prev.push(item)
        //     } else if (item.key.substring(0, 2) === 'RI') {
        //       if (item.key === _last(prev).key) {
        //         _last(prev).value.push(item.value[0])
        //       } else {
        //         prev.push(item)
        //       }
        //     } else if (item.key.substring(0, 2) === 'TF') {
        //       if (item.key === _last(prev).key) {
        //         _last(prev).value = _last(prev).value.concat(item.value)
        //       } else {
        //         prev.push(item)
        //       }
        //     }
        //     return prev
        //   }, [])

        async.eachSeries(
          dbInstructions,
          function (item, callback) {
            Indexer.options.indexes.get(item.key, function (err, val) {
              if (err) log.info(err)
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
            if (err) log.info(err)
            dbInstructions.push({key: 'LAST-UPDATE-TIMESTAMP', value: Date.now()})
            Indexer.options.indexes.batch(dbInstructions, function (err) {
              if (err) log.info('Ooops!', err)
              else log.info('batch indexed!')
              return callbackster(null)
            })
          })
      }

      //  return Indexer
      return callback(null, Indexer)
    })
  })
}

var getOptions = function(givenOptions, callbacky) {
  givenOptions = givenOptions || {}
  async.parallel([
    function(callback) {
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
    function(callback){
      if (!givenOptions.indexes) {
        levelup(givenOptions.indexPath || 'si', {
          valueEncoding: 'json'
        }, function(err, db) {
          callback(null, db)          
        })
      }
      else {
        callback(null, null)
      }
    }
  ], function(err, results){
    var options = _defaults(givenOptions, results[0])
    if (results[1] != null) {
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
  batchOptions = _defaults(batchOptions || {}, defaultBatchOptions)
  batchOptions.filters = _map(_filter(batchOptions.fieldOptions, 'filter'), 'fieldName')
  if (_find(batchOptions.fieldOptions, ['fieldName', '*']) === -1) {
    batchOptions.fieldOptions.push(defaultFieldOptions('*'))
  }
  return batchOptions
}

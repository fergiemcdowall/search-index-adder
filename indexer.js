/*jshint -W083 */ //makes jslint overlook functions in lodash for-loops
const _ = require('lodash');

module.exports = function (options) {
  var Indexer = {};
  Indexer.options = getOptions(options)

  var async = require('async');
  var deleter = require('search-index-deleter')(Indexer.options);
  var hash = require('object-hash');
  var tv = require('term-vector');
  var tf = require('term-frequency');
  var skeleton = require('log-skeleton');

  var log = skeleton((Indexer.options) ? Indexer.options.log : undefined);


  var q = async.queue(function (batch, callback) {
    batch.options = _.defaults(batch.options, options);
    //generate IDs if none are present and stringify numeric IDs
    var salt = 0;
    batch.data.map(function (doc) {
      if (!doc.id)
        doc.id = (++salt) + '-' + hash(doc);
      doc.id = doc.id + ''; // stringify ID
    });
    deleter.deleteBatch(_.pluck(batch.data, 'id'), function (err) {
      if (err) log.info(err);
      addBatch(batch.data, batch.options, function(err) {
        return callback(err);
      });
    });
  }, 1);


  Indexer.add = function (batch, batchOptions, callback) {
    if (arguments.length === 2 && _.isFunction(arguments[1])) {
      callback = batchOptions
      batchOptions = undefined
    }
    addBatchToIndex(batch,
                    batchOptions,
                    callback)
  }


  Indexer.getOptions = function() {
    return Indexer.options
  }
  
  var addBatchToIndex = function (batch, batchOptions, callback) {
    batchOptions = processBatchOptions(Indexer.options, batchOptions);
    if (!_.isArray(batch) && _.isPlainObject(batch)) {
      batch = [batch];
    }
    q.push({data:batch, options:batchOptions}, callback)
  };


  var removeInvalidFields = function (doc) {
    for (var fieldKey in doc) {
      if (_.isArray(doc[fieldKey])) continue;
      else if (doc[fieldKey] === null) {
        delete doc[fieldKey];
        log.warn(doc.id + ': ' + fieldKey + ' field is null, SKIPPING');
      }
      //only index fields that are strings or numbers
      else if (!(_.isString(doc[fieldKey]) || _.isNumber(doc[fieldKey]))) {
        delete doc[fieldKey];
        log.warn(doc.id + ': ' + fieldKey +
                 ' field not string or array, SKIPPING');
      }
    }
    return doc;
  };


  function getIndexEntries(doc, batchOptions) {
    var docIndexEntries = [];
    if (!_.isPlainObject(doc))
      return callbacky(new Error('Malformed document'), {});
    doc = removeInvalidFields(doc);
    if (batchOptions.fieldsToStore == 'all')
      batchOptions.fieldsToStore = Object.keys(doc);
    log.info('indexing ' + doc.id);
    docIndexEntries.push({
      type: 'put',
      key: 'DOCUMENT￮' + doc.id + '￮',
      value:  _.pick(doc, batchOptions.fieldsToStore)
    });
    var freqsForComposite = []; //put document frequencies in here
    _.forEach(doc, function (field, fieldName) {
      var fieldOptions = _.defaults(_.find(batchOptions.fieldOptions, 'fieldName', fieldName) || {}, batchOptions.defaultFieldOptions);
      if (fieldName == 'id') fieldOptions.stopwords = '';   // because you cant run stopwords on id field
      else fieldOptions.stopwords = batchOptions.stopwords;
      if (_.isArray(field)) field = field.join(' '); // make filter fields searchable
      var v = tv.getVector(field + '', {
        separator: batchOptions.separator,
        stopwords: fieldOptions.stopwords,
        nGramLength: fieldOptions.nGramLength
      });
      v.push(['*', 1]); //can do wildcard searh on this field
      var freq = tf.getTermFrequency(v, {
        scheme: 'doubleLogNormalization0.5',
        weight: fieldOptions.weight
      });
      if (fieldOptions.searchable)
        freqsForComposite.push(freq);
      var deleteKeys = [];
      if (fieldOptions.fieldedSearch) {
        freq.forEach(function (item) {
          batchOptions.filters.forEach(function (filter) {
            _.forEach(doc[filter], function (filterKey) {
              docIndexEntries.push({
                type: 'put',
                key: 'TF￮' + fieldName + '￮' + item[0] + '￮' + filter + '￮' + filterKey,
                value: [doc.id]
              });
              docIndexEntries.push({
                type: 'put',
                key: 'RI￮' + fieldName + '￮' + item[0] + '￮' + filter + '￮' + filterKey,
                value: [[item[1].toFixed(16), doc.id]]
              });
            });
          });
          docIndexEntries.push({
            type: 'put',
            key: 'TF￮' + fieldName + '￮' + item[0] + '￮￮',
            value: [doc.id]
          });
          docIndexEntries.push({
            type: 'put',
            key: 'RI￮' + fieldName + '￮' + item[0] + '￮￮',
            value: [[item[1].toFixed(16), doc.id]]
          });
        });
      };
    });
    //generate * field
    _(freqsForComposite)
      .flatten()
      .sort()
      .reduce(function (prev, item) {
        if (!prev[0]) prev.push(item);
        else if (item[0] == _.last(prev)[0]) {
          _.last(prev)[1] = _.last(prev)[1] + item[1];
        }
        else
          prev.push(item);
        return prev;
      }, [])
      .forEach(function (item) {
        batchOptions.filters.forEach(function (filter) {
          _.forEach(doc[filter], function (filterKey) {
            docIndexEntries.push({
              type: 'put',
              key: 'TF￮*￮' + item[0] + '￮' + filter + '￮' + filterKey,
              value: [doc.id]
            });
            docIndexEntries.push({
              type: 'put',
              key: 'RI￮*￮' + item[0] + '￮' + filter + '￮' + filterKey,
              value: [[item[1].toFixed(16), doc.id]]
            });
          });
        });
        docIndexEntries.push({
          type: 'put',
          key: 'TF￮*￮' + item[0] + '￮￮',
          value: [doc.id]
        });
        docIndexEntries.push({
          type: 'put',
          key: 'RI￮*￮' + item[0] + '￮￮',
          value: [[item[1].toFixed(16), doc.id]]
        });
      });
    docIndexEntries.push({
      type: 'put',
      key: 'DELETE-DOCUMENT￮' + doc.id,
      value: _.pluck(docIndexEntries, 'key')
    });
    return docIndexEntries;
  }

  function addBatch(batch, batchOptions, callbackster) {
    var dbInstructions = [];
    batch.forEach(function (doc) {
      dbInstructions.push(getIndexEntries(doc, batchOptions));
    });
    dbInstructions.push({
      type: 'put',
      key: 'DOCUMENT-COUNT',
      value: batch.length
    });
    dbInstructions = _(dbInstructions)
      .flatten()
      .sortBy('key')
      .reduce(function (prev, item) {
        if (item.key.substring(0, 6) == 'DELETE')
          prev.push(item);
        else if (item.key.substring(0, 8) == 'DOCUMENT')
          prev.push(item);
        else if (item.key.substring(0, 2) == 'RI') {
          if (item.key == _.last(prev).key)
            _.last(prev).value.push(item.value[0]);
          else
            prev.push(item);
        }
        else if (item.key.substring(0, 2) == 'TF') {
          if (item.key == _.last(prev).key)
            _.last(prev).value = _.last(prev).value.concat(item.value);
          else
            prev.push(item);
        }
        return prev;
      }, []);
    async.eachSeries(
      dbInstructions,
      function (item, callback) {
        Indexer.options.indexes.get(item.key, function (err, val) {
          if (item.key.substring(0, 2) == 'TF') {
            if (val)
              item.value = item.value.concat(val);
            item.value = item.value.sort();
          }
          else if (item.key.substring(0, 2) == 'RI') {
            if (val)
              item.value = item.value.concat(val);
            item.value = item.value.sort(function (a, b) {
              //sort buy score and then ID, descending:
              if (b[0] > a[0]) return 1
              if (b[0] < a[0]) return -1
              if (b[1] > a[1]) return 1
              if (b[1] < a[1]) return -1
              return 0
            });
          }
          else if (item.key == 'DOCUMENT-COUNT') {
            if (val)
              item.value = +val + +(item.value);
          }
          return callback(null);
        });
      },
      function (err) {
        Indexer.options.indexes.batch(dbInstructions, function (err) {
          if (err) log.warn('Ooops!', err);
          else log.info('batch indexed!');
          return callbackster(null);
        });
      });
  }

  return Indexer;
};

var getOptions = function(options) {
  const bunyan = require('bunyan')
  const level = require('levelup')
  const tv = require('term-vector')
  var newOptions = {}
  var defaults = {
    deletable: true,
    fieldedSearch: true,
    fieldsToStore: 'all',
    indexPath: 'si',
    logLevel: 'error',
    nGramLength: 1,
    separator: /[\|' \.,\-|(\n)]+/,
    stopwords: tv.getStopwords('en').sort(),
  }
  // initialize defaults options
  newOptions = _.clone(_.defaults(options || {}, defaults))
  newOptions.log = options.log || bunyan.createLogger({
    name: 'search-index',
    level: newOptions.logLevel
  })
  newOptions.indexes = options.indexes || level(newOptions.indexPath, {
    valueEncoding: 'json',
    db: newOptions.db
  })
  return newOptions;
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
  batchOptions.filters = _.pluck(_.filter(batchOptions.fieldOptions, 'filter'), 'fieldName')
  if (_.find(batchOptions.fieldOptions, 'fieldName', '*') === -1) {
    batchOptions.fieldOptions.push(defaultFieldOptions('*'))
  }
  return batchOptions
}

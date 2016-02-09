var _ = require('lodash');
var da = require('distribute-array')
var sia = require('../')
var sis = require('search-index-searcher')
var test = require('tape');
var async = require('async');


var resultForStarUSA = [ '287', '510', '998', '997', '996', '995', '994', '993', '992', '991' ]


test('simple indexing test', function (t) {
  var indexer = sia({indexPath: 'test/sandbox/simpleIndexing'});
  var searcher = sis(indexer.getOptions())
  t.plan(4);
  var batch = require('../node_modules/reuters-21578-json/data/full/reuters-000.json')
  t.equal(batch.length, 1000);
  indexer.addBatchToIndex(batch, {}, function(err) {
    if (!err) t.pass('no errorness')
    var q = {}
    q.query = {'*': ['usa']}
    searcher.search(q, function (err, searchResults) {
      if (!err) t.pass('no errorness')
      t.deepLooseEqual(_.map(searchResults.hits, 'id').slice(0,10), resultForStarUSA)
    })
  })
});


test('concurrancy test', function (t) {
  var indexer = sia({indexPath: 'test/sandbox/concurrentIndexing'});
  var searcher = sis(indexer.getOptions())
  t.plan(13);
  var batchData = da(require('../node_modules/reuters-21578-json/data/full/reuters-000.json'), 10)
  t.equal(batchData.length, 10);
  async.each(batchData, function(batch, callback) {
    console.log('task submitted')
    indexer.addBatchToIndex(batch, {}, function(err) {
      if (!err) t.pass('no errorness')
      callback();
    })    
  }, function(err) {
    var q = {}
    q.query = {'*': ['usa']} // TODO: add error message if this is
    //      not an array
    searcher.search(q, function (err, searchResults) {
      if (!err) t.pass('no errorness')
      t.deepLooseEqual(_.map(searchResults.hits, 'id').slice(0,10), resultForStarUSA)
    })
  })
});

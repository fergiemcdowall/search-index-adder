var _ = require('lodash')
var da = require('distribute-array')
var sia = require('../')
var sis = require('search-index-searcher')
var test = require('tape')
var async = require('async')

var resultForStarUSA = [ '510', '287', '998', '997', '996', '995', '994', '993', '992', '991' ]

test('set seperator at field level', function (t) {
  t.plan(5)
  var batch = [{
    id: '1',
    title: 'thisxisxaxtitle',
    body: 'horsexzebraxelephant'
  }, {
    id: '2',
    title: 'this is a title',
    body: 'horse zebra elephant'
  }]
  sia({
    indexPath: 'test/sandbox/separatorTest'
  }, function (err, indexer) {
    t.error(err)
    indexer.add(batch, {
      fieldOptions: [
        {
          fieldName: 'body',
          separator: 'x'
        }
      ]
    }, function (err) {
      t.error(err)
      sis(indexer.options, function (err, searcher) {
        t.error(err)
        var q = {}
        q.query = {
          AND: {'*': ['zebra']}
        }
        searcher.search(q, function (err, searchResults) {
          t.error(err)
          t.equal(searchResults.hits[0].id, '1')
        })
      })
    })
  })
})

test('simple indexing test - debugging 510', function (t) {
  t.plan(10)
  var batch = _.filter(require('../node_modules/reuters-21578-json/data/full/reuters-000.json'), { 'id': '510' })
  t.equal(batch.length, 1)
  sia({indexPath: 'test/sandbox/simpleIndexing510'}, function (err, indexer) {
    t.error(err)
    sis(indexer.options, function (err, searcher) {
      t.error(err)
      indexer.add(batch, {}, function (err) {
        t.error(err)
        var q = {}
        q.query = {
          AND: {'*': ['usa']}
        }
        searcher.search(q, function (err, searchResults) {
          t.error(err)
          t.equal(searchResults.hits[0].tfidf[0][0][0], 'usa')
          t.equal(searchResults.hits[0].tfidf[0][0][1], '*')
          t.equal(searchResults.hits[0].tfidf[0][0][2], 0.545144315135374)
          t.equal(searchResults.hits[0].tfidf[0][0][3], 1.8109302162163288)
          t.equal(searchResults.hits[0].tfidf[0][0][4], 0.3010299956639812)
        })
      })
    })
  })
})

test('simple indexing test', function (t) {
  var batch = require('../node_modules/reuters-21578-json/data/full/reuters-000.json')
  t.plan(6)
  t.equal(batch.length, 1000)
  sia({indexPath: 'test/sandbox/simpleIndexing'}, function (err, indexer) {
    t.error(err)
    sis(indexer.options, function (err, searcher) {
      t.error(err)
      indexer.add(batch, {}, function (err) {
        t.error(err)
        var q = {}
        q.query = {
          AND: {'*': ['usa']}
        }
        searcher.search(q, function (err, searchResults) {
          t.error(err)
          t.deepLooseEqual(_.map(searchResults.hits, 'id').slice(0, 10), resultForStarUSA)
        })
      })
    })
  })
})

test('concurrancy test', function (t) {
  var startTime = Date.now()
  t.plan(19)
  sia({indexPath: 'test/sandbox/concurrentIndexing'}, function (err, indexer) {
    t.error(err)
    sis(indexer.options, function (err, searcher) {
      t.error(err)
      var batchData = da(require('../node_modules/reuters-21578-json/data/full/reuters-000.json'), 10)
      t.equal(batchData.length, 10)
      async.each(batchData, function (batch, callback) {
        console.log('task submitted')
        indexer.add(batch, {}, function (err) {
          if (!err) t.pass('no errorness')
          callback()
        })
      }, function (err) {
        t.error(err)
        var q = {}
        q.query = {
          AND: {'*': ['usa']}
        }
        searcher.search(q, function (err, searchResults) {
          if (!err) t.pass('no errorness')
          t.deepLooseEqual(_.map(searchResults.hits, 'id').slice(0, 10), resultForStarUSA)
        })
        indexer.options.indexes.get('LAST-UPDATE-TIMESTAMP', function (err, val) {
          t.error(err)
          t.ok((val - startTime) > 0,
            'lastUpdateTimestamp seems reasonable (' + (val - startTime) + ')')
          t.ok((val - startTime) < 60000,
            'lastUpdateTimestamp seems reasonable (' + (val - startTime) + ')')
        })
      })
    })
  })
})

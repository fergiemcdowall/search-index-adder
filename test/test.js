var _ = require('lodash')
var da = require('distribute-array')
var SearchIndexAdder = require('../')
var SearchIndexSearcher = require('search-index-searcher')
var test = require('tape')
var async = require('async')

var resultsForStarUSA = [
  '998',
  '997',
  '996',
  '995',
  '994',
  '993',
  '992',
  '991',
  '510',
  '287'
]

test('set seperator at field level', function (t) {
  t.plan(4)
  var batch = [{
    id: '1',
    title: 'thisxisxaxtitle',
    body: 'horsexzebraxelephant'
  }, {
    id: '2',
    title: 'this is a title',
    body: 'horse zebra elephant'
  }]
  SearchIndexAdder({
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
      SearchIndexSearcher(indexer.options, function (err, searcher) {
        t.error(err)
        var q = {}
        q.query = [{
          AND: {'*': ['zebra']}
        }]
        searcher.search(q)
          .on('data', function (data) {
            console.log(JSON.parse(data).document.id)
            t.ok(JSON.parse(data).document.id === '1')
          })
      })
    })
  })
})

test('simple indexing test - debugging 510', function (t) {
  t.plan(5)
  var batch = _.filter(require('../node_modules/reuters-21578-json/data/full/reuters-000.json'), { 'id': '510' })
  t.equal(batch.length, 1)
  SearchIndexAdder({indexPath: 'test/sandbox/simpleIndexing510'}, function (err, indexer) {
    t.error(err)
    SearchIndexSearcher(indexer.options, function (err, searcher) {
      t.error(err)
      indexer.add(batch, {}, function (err) {
        t.error(err)
        var q = {}
        q.query = {
          AND: {'*': ['usa']}
        }
        searcher.search(q)
          .on('data', function (data) {
            data = JSON.parse(data)
            t.looseEqual(data.score, {
              'tf': {
                '*￮usa': 1
              },
              'df': {
                '*￮usa': 1.8109302162163288
              },
              'tfidf': {
                '*￮usa': 0.19094834880611658
              },
              'score': 0.19094834880611658
            })
          })
      })
    })
  })
})

test('simple indexing test', function (t) {
  var batch = require('../node_modules/reuters-21578-json/data/full/reuters-000.json')
  t.plan(14)
  t.equal(batch.length, 1000)
  SearchIndexAdder({indexPath: 'test/sandbox/simpleIndexing'}, function (err, indexer) {
    t.error(err)
    SearchIndexSearcher(indexer.options, function (err, searcher) {
      t.error(err)
      indexer.add(batch, {}, function (err) {
        t.error(err)
        var q = {}
        q.query = {
          AND: {'*': ['usa']}
        }
        q.pageSize = 10
        var i = 0
        searcher.search(q).on('data', function (data) {
          data = JSON.parse(data)
          console.log(data.document.id)
          t.ok(resultsForStarUSA[i++] === data.document.id)
        })
      })
    })
  })
})

test('concurrancy test', function (t) {
  var startTime = Date.now()
  t.plan(27)
  SearchIndexAdder({indexPath: 'test/sandbox/concurrentIndexing'}, function (err, indexer) {
    t.error(err)
    SearchIndexSearcher(indexer.options, function (err, searcher) {
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
        q.pageSize = 10
        var i = 0
        searcher.search(q).on('data', function (data) {
          data = JSON.parse(data)
          console.log(data.document.id)
          t.ok(resultsForStarUSA[i++] === data.document.id)
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

// TODO: make this work again

// test('preserve array fields in stored document', function (t) {
//   t.plan(6)
//   SearchIndexAdder({indexPath: 'test/sandbox/preserveArrayFields'}, function (err, indexer) {
//     t.error(err)
//     SearchIndexSearcher(indexer.options, function (err, searcher) {
//       t.error(err)
//       indexer.add([{'id': '1', 'anArray': ['one', 'two', 'three']}], function (err) {
//         console.log('XXXXXXXXXXXXXXXXX')
//         var q = {}
//         if (!err) t.pass('no errors')
//         q.query = {
//           AND: {'*': ['one']}
//         }

//         // searcher.search(q, function (err, searchResults) {
//         //   if (!err) t.pass('no errors')
//         //   t.equal(searchResults.hits.length, 1)
//         //   t.deepEqual(searchResults.hits[0].document, {'id': '1', 'anArray': ['one', 'two', 'three']})
//         // })
//         searcher.search(q).on('data', function(data) {
//           console.log(data)
//         })

//       })
//     })
//   })
// })

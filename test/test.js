var JSONStream = require('JSONStream')
var Readable = require('stream').Readable
var SearchIndexAdder = require('../')
var SearchIndexSearcher = require('search-index-searcher')
var fs = require('fs')
var test = require('tape')

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
  const s = new Readable({ objectMode: true })
  batch.forEach(function (elem) {
    s.push(elem)
  })
  s.push(null)
  SearchIndexAdder({
    indexPath: 'test/sandbox/separatorTest'
  }, function (err, indexer) {
    t.error(err)
    s.pipe(indexer.defaultPipeline())
      .on('data', function (data) {
        t.looseEqual(
          Object.keys(data),
          [ 'normalised', 'options', 'raw', 'stored', 'tokenised', 'vector', 'id' ])
      })
      .pipe(indexer.add())
      .on('data', function (data) {})
      .on('end', function () {
        indexer.close(function (err) {
          t.error(err)
          SearchIndexSearcher(indexer.options, function (err, searcher) {
            t.error(err)
            var q = {}
            q.query = [{
              AND: {'*': ['zebra']}
            }]
            searcher.search(q)
              .on('data', function (data) {
                t.ok(data.document.id === '1')
              })
          })
        })
      })
  })
})

test('simple indexing test', function (t) {
  t.plan(5)
  var j = 0
  var k = 0
  SearchIndexAdder({
    indexPath: 'test/sandbox/simpleIndexing'
  }, function (err, indexer) {
    t.error(err)
    fs.createReadStream('./node_modules/reuters-21578-json/data/fullFileStream/000.str')
      .pipe(JSONStream.parse())
      .on('data', function (data) {
        j++
      })
      .pipe(indexer.defaultPipeline())
      .on('data', function (data) {
        k++
      })
      .pipe(indexer.add())
      .on('data', function (data) {})
      .on('end', function () {
        indexer.close(function (err) {
          t.error(err)
          t.equals(j, 1000)
          t.equals(k, 1000)
          SearchIndexSearcher(indexer.options, function (err, searcher) {
            t.error(err)
            var q = {}
            q.query = {
              AND: {'*': ['usa']}
            }
            q.pageSize = 10
            var i = 0
            searcher.search(q).on('data', function (data) {
              console.log(data.document.id)
              t.equals(resultsForStarUSA[i++], data.document.id)
            })
          })
        })
      })
  })
})

// TODO: make this work again

test('preserve array fields in stored document', function (t) {
  t.plan(5)
  SearchIndexAdder({indexPath: 'test/sandbox/preserveArrayFields'}, function (err, indexer) {
    t.error(err)
    SearchIndexSearcher(indexer.options, function (err, searcher) {
      t.error(err)
      const s = new Readable({ objectMode: true })
      s.push({'id': '1', 'anArray': ['one', 'two', 'three']})
      s.push(null)
      s.pipe(indexer.defaultPipeline())
        .pipe(indexer.add())
        .on('data', function (data) {})
        .on('end', function () {
          var q = {}
          q.query = {
            AND: {'*': ['one']}
          }
          searcher.search(q)
            .on('data', function (data) {
              t.equals(data.document.id, '1')
              t.looseEquals(data.document.anArray, ['one', 'two', 'three'])
            })
            .on('end', function () {
              t.ok(true)
            })
        })
    })
  })
})

var _ = require('lodash')
var da = require('distribute-array')
var SearchIndexAdder = require('../')
var SearchIndexSearcher = require('search-index-searcher')
var test = require('tape')
var async = require('async')
var Readable = require('stream').Readable
var JSONStream = require('JSONStream')
var fs = require('fs')

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
  const s = new Readable()
  batch.forEach(function (elem) {
    s.push(JSON.stringify(elem))
  })
  s.push(null)
  SearchIndexAdder({
    indexPath: 'test/sandbox/separatorTest'
  }, function (err, indexer) {
    s.pipe(JSONStream.parse())
      .pipe(indexer.defaultPipeline())
      .pipe(indexer.createWriteStream2())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
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
                console.log(JSON.parse(data).document.id)
                t.ok(JSON.parse(data).document.id === '1')
              })
          })
        })
      })
  })
})



test('simple indexing test', function (t) {
  // var batch = require('../node_modules/reuters-21578-json/data/full/reuters-000.json')
  t.plan(1002)
  // t.equal(batch.length, 1000)

  SearchIndexAdder({
    indexPath: 'test/sandbox/simpleIndexing'
  }, function (err, indexer) {
    fs.createReadStream('./node_modules/reuters-21578-json/data/fullFileStream/000.str')
      .pipe(JSONStream.parse())
      .pipe(indexer.defaultPipeline())
      .pipe(indexer.createWriteStream2())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        indexer.close(function (err) {
          SearchIndexSearcher(indexer.options, function (err, searcher) {
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

      const s = new Readable()
      s.push(JSON.stringify({'id': '1', 'anArray': ['one', 'two', 'three']}))
      s.push(null)

      s.pipe(JSONStream.parse())
        .pipe(indexer.defaultPipeline())
        .pipe(indexer.createWriteStream2())
        .on('data', function(data) {})
        .on('end', function () {
          var q = {}
          q.query = {
            AND: {'*': ['one']}
          }
          searcher.search(q)
            .on('data', function(data) {
              data = JSON.parse(data)
              t.equals(data.document.id, '1')
              t.looseEquals(data.document.anArray, ['one', 'two', 'three'])
            })
            .on('end', function() {
              t.ok(true)
            })
        })

    })
  })
})

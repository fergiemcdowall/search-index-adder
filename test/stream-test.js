
var fs = require('fs')
var sia = require('../')
var SearchIndex = require('search-index')
var test = require('tape')
var JSONStream = require('JSONStream')
var indexer


test('init indexer', function (t) {
  t.plan(1)
  sia({
    indexPath: 'test/sandbox/streamTest'
  }, function (err, thisIndexer) {
    indexer = thisIndexer
    t.error(err)
  })
})


test('test stream file', function (t) {
  t.plan (11)
  const filePath = '../reuters-21578-json/data/fullFileStream/justTen.str'
  require('readline').createInterface({
    input: fs.createReadStream(filePath)
  })
    .on ('line', function(line) {
      t.ok(true)
    })
    .on ('end', function() {
      t.ok(true)
    })
})


test('stream file to search-index', { timeout: 6000000 }, function (t) {
  t.plan (12)
  const filePath = '../reuters-21578-json/data/fullFileStream/justTen.str'
    fs.createReadStream(filePath)
    .pipe(JSONStream.parse())
    .pipe(indexer.createWriteStream())
    .on('data', function(data) {
      t.ok(true)
    }).on('end', function() {
      console.log('test completed')
      t.ok(true)
    })
})

test('close search-index-adder', function (t) {
  t.plan (1)
  indexer.close(function(err) {
    t.ok(true)
  })
})

test('index should be searchable', function (t) {
  t.plan(3)
  SearchIndex({
    indexPath: 'test/sandbox/streamTest'
  }, function (err, si) {
    t.error(err)
    si.search({
      query: {
        AND: [{'*': ['*']}]
      }
    }, function (err, results) {
      t.error(err)
      t.looseEqual(
        results.hits.map(function (item) { return item.id }),
        [ '9', '8', '7', '6', '5', '4', '3', '2', '10', '1' ]
      )
    })
  })
})



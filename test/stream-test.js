var fs = require('fs')
var sia = require('../')
var SearchIndexSearcher = require('search-index-searcher')
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
  t.plan(11)
  const filePath = './node_modules/reuters-21578-json/data/fullFileStream/justTen.str'
  // const filePath = './data.json'
  require('readline').createInterface({
    input: fs.createReadStream(filePath)
  })
    .on('line', function (line) {
      t.ok(true)
    })
    .on('close', function () {
      t.ok(true)
    })
})

test('stream file to search-index', { timeout: 6000000 }, function (t) {
  t.plan(12)
  const filePath = './node_modules/reuters-21578-json/data/fullFileStream/justTen.str'
  // const filePath = './data.json'
  fs.createReadStream(filePath)
    .pipe(JSONStream.parse())
    .pipe(indexer.defaultPipeline())
    .pipe(indexer.add())
    .on('data', function (data) {
      t.ok(true)
    }).on('end', function () {
      console.log('test completed')
      t.ok(true)
    })
})

test('close search-index-adder', function (t) {
  t.plan(1)
  indexer.close(function (err) {
    t.error(err)
  })
})

test('index should be searchable', function (t) {
  t.plan(11)
  var results = [ '9', '8', '7', '6', '5', '4', '3', '2', '10', '1' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/streamTest'
  }, function (err, si) {
    t.error(err)
    si.search({
      query: [{
        AND: {'*': ['*']}
      }]
    }).on('data', function (data) {
      t.ok(JSON.parse(data).document.id === results.shift())
    })
  })
})

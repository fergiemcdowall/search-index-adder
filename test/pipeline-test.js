const JSONStream = require('JSONStream')
const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const fs = require('fs')
const sw = require('stopword')
const test = require('tape')

var indexer

test('init indexer', function (t) {
  t.plan(1)
  SearchIndexAdder({
    indexPath: 'test/sandbox/pipelineTest'
  }, function (err, thisIndexer) {
    indexer = thisIndexer
    t.error(err)
  })
})

test('test stream file', function (t) {
  t.plan(11)
  // const filePath = './node_modules/reuters-21578-json/data/fullFileStream/justTen.str'
  const filePath = './test/data.json'
  fs.createReadStream(filePath)
    .pipe(JSONStream.parse())
    .on('data', function (data) {
      t.ok(true)
    })
    .on('close', function () {
      t.ok(true)
    })
    .on('error', function (err) {
      console.log(err)
      t.error(err)
    })
})

test('transform stream file', { timeout: 6000000 }, function (t) {
  t.plan(11)
  const filePath = './test/data.json'
  fs.createReadStream(filePath)
    .pipe(JSONStream.parse())
    .pipe(indexer.defaultPipeline({
      ngram: [1],
      stopwords: sw.en,
      // searchable, storeable, vectorType
      fieldOptions: {
        body: {
          searchable: true,
          storeable: true
        },
        price: {
          sortable: true
        }
      }
    }))
    .pipe(indexer.add())
    .on('data', function (data) {
      t.ok(true)
    })
    .on('close', function () {
      t.ok(true)
    })
})

test('closes indexer', function (t) {
  t.plan(1)
  indexer.close(function (err) {
    t.error(err)
  })
})

test('can search', function (t) {
  t.plan(7)
  var results = [ '5', '4', '3', '2', '10' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/pipelineTest'
  }, function (err, si) {
    t.error(err)
    si.search(
      {
        query: [
          {
            AND: {'name': ['swiss']}
          }
        ]
      }
    ).on('data', function (data) {
      data = JSON.parse(data)
      t.ok(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('can search', function (t) {
  t.plan(8)
  var results = [ '9', '5', '4', '3', '2', '10' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/pipelineTest'
  }, function (err, si) {
    t.error(err)
    si.search(
      {
        query: [
          {
            AND: {'*': ['swiss']}
          }
        ]
      }
    ).on('data', function (data) {
      data = JSON.parse(data)
      t.equal(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('can search', function (t) {
  t.plan(12)
  var results = [ '9', '8', '7', '6', '5', '4', '3', '2', '10', '1' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/pipelineTest'
  }, function (err, si) {
    t.error(err)
    si.search(
      {
        query: [
          {
            AND: {'*': ['*']}
          }
        ]
      }
    ).on('data', function (data) {
      data = JSON.parse(data)
      t.equal(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('can search for id', function (t) {
  t.plan(3)
  var results = [ '9' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/pipelineTest'
  }, function (err, si) {
    t.error(err)
    si.search(
      {
        query: [
          {
            AND: {'id': ['9']}
          }
        ]
      }
    ).on('data', function (data) {
      console.log('BOOOOOOOOOOOM')
      data = JSON.parse(data)
      t.equal(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

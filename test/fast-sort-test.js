const sandbox = 'test/sandbox/'
const test = require('tape')
const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const num = require('written-number')
const Readable = require('stream').Readable
const batchSize = 10
const levelup = require('levelup')

var sia, sis

test('initialize a search index', t => {
  t.plan(3)
  levelup(sandbox + 'fast-sort-test', {
    valueEncoding: 'json'
  }, function (err, db) {
    t.error(err)
    SearchIndexAdder({
      indexes: db
    }, (err, newSi) => {
      sia = newSi
      t.error(err)
    })
    SearchIndexSearcher({
      indexes: db
    }, (err, newSi) => {
      sis = newSi
      t.error(err)
    })
  })
})

test('make an index with storeDocument: false', t => {
  t.plan(1)
  var s = new Readable({ objectMode: true })
  for (var i = 1; i <= batchSize; i++) {
    s.push({
      id: i,
      tokens: 'this is the amazing doc number ' + num(i)
    })
  }
  s.push(null)
  s.pipe(sia.feed({
    objectMode: true,
    wildcard: false,
    compositeField: false,
    storeDocument: false,
    fastSort: false
  }))
    .on('finish', function () {
      t.pass('finished')
    })
    .on('error', function (err) {
      t.error(err)
    })
})

test('search works', t => {
  t.plan(10)
  var results = ['9', '8', '7', '6', '5', '4', '3', '2', '10', '1']
  sis.search({
    query: [
      {
        AND: {
          tokens: ['amazing']
        }
      }
    ]
  })
    .on('data', function (d) {
      t.equal(d.id, results.shift())
    })
    .on('error', function (err) {
      t.error(err)
    })
})

test('paging works', t => {
  t.plan(5)
  var results = ['7', '6', '5', '4', '3']
  sis.search({
    offset: 2,
    pageSize: 5,
    query: [
      {
        AND: {
          tokens: ['amazing']
        }
      }
    ]
  })
    .on('data', function (d) {
      t.equal(d.id, results.shift())
    })
    .on('error', function (err) {
      t.error(err)
    })
})

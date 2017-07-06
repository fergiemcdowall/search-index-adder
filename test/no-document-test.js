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
  levelup(sandbox + 'no-doc-test', {
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

test('make an index', t => {
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
    storeDocument: false,
    wildcard: false,
    compositeField: false
  }))
    .on('finish', function () {
      t.pass('finished')
    })
    .on('error', function (err) {
      t.error(err)
    })
})

test('get all docs', t => {
  t.plan(10)
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
      t.pass('got result')
    })
    .on('error', function (err) {
      t.error(err)
    })
})

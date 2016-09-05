const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const test = require('tape')
const JSONStream = require('JSONStream')
const Readable = require('stream').Readable

const data = [
  {
    id: 'one',
    text: 'the first doc'
  },
  {
    id: 'two',
    text: 'the second doc'
  },
  {
    id: 'three',
    text: 'the third doc'
  },
  {
    id: 'four',
    text: 'the fourth doc'
  }  
]


test('make the search index', function (t) {
  t.plan(7)
  const s = new Readable()
  data.forEach(function (stone) {
    s.push(JSON.stringify(stone))
  })
  s.push(null)
  SearchIndexAdder({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    s.pipe(JSONStream.parse())
      .pipe(si.defaultPipeline())
      .pipe(si.add())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        si.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('confirm can search as normal', function (t) {
  t.plan(6)
  var results = [ 'two', 'three', 'one', 'four' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    si.search({
      AND: {'*': ['*']}
    }).on('data', function (data) {
      data = JSON.parse(data)
      t.equals(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('can delete', function (t) {
  t.plan(2)
  SearchIndexAdder({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    si.deleter(['one'])
      .on('data', function (data) {
        console.log(data)
      })
      .on('end', function () {
        si.close(function(err) {
          t.error(err)
        })
      })
  })
})

test('confirm can search with document deleted', function (t) {
  t.plan(5)
  var results = [ 'two', 'three', 'four' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    si.search({
      AND: {'*': ['*']}
    }).on('data', function (data) {
      data = JSON.parse(data)
      t.equals(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

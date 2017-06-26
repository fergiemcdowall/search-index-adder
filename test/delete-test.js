const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const test = require('tape')
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
  t.plan(2)
  const s = new Readable({ objectMode: true })
  data.forEach(function (stone) {
    s.push(stone)
  })
  s.push(null)
  SearchIndexAdder({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    s.pipe(si.feed({ objectMode: true }))
      .on('data', function (data) {})
      .on('finish', function () {
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
      t.equals(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('can delete', function (t) {
  t.plan(3)
  SearchIndexAdder({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    si.deleter(['one'], function (err) {
      t.error(err)
      si.close(function (err) {
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
      // console.log(JSON.stringify(data, null, 2))
      t.equals(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('can delete 2 docs', function (t) {
  t.plan(3)
  SearchIndexAdder({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    si.deleter(['two', 'four'], function (err) {
      t.error(err)
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('confirm can search with 3 docs deleted', function (t) {
  t.plan(3)
  var results = [ 'three' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/deleteTest'
  }, function (err, si) {
    t.error(err)
    si.search({
      AND: {'*': ['*']}
    }).on('data', function (data) {
      // console.log(JSON.stringify(data, null, 2))
      t.equals(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

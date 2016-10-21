const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const test = require('tape')

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

test('make the search index using the callback API', function (t) {
  t.plan(3)
  SearchIndexAdder({
    indexPath: 'test/sandbox/callbackTest'
  }, function (err, si) {
    t.error(err)
    si.callbackyAdd({}, data, function (err) {
      t.error(err)
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('confirm can search as normal using the streaming API', function (t) {
  t.plan(6)
  var results = [ 'two', 'three', 'one', 'four' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/callbackTest'
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

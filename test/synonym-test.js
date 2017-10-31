const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const test = require('tape')
const Readable = require('stream').Readable
const fs = require('fs')
const indexPath = 'test/sandbox/synonymTest'

const data = [
  {
    id: 'one',
    title: 'The Big Title oppsigelse',
    text: 'the first doc L20050617-62 is about usakelig oppsigelse'
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
    indexPath: indexPath,
    separator: ' '
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

test('Add synonyms', function (t) {
  t.plan(2)
  var synonyms = JSON.parse(fs.readFileSync('./test/synonyms.json', 'utf8'))
  SearchIndexAdder({
    indexPath: indexPath
  }, function (err, index) {
    t.error(err)
    index.synonyms(synonyms, err => {
      t.error(err)      
    })
  })
})


test('confirm can search as normal', function (t) {
  t.plan(3)
  var results = [ 'one' ]
  SearchIndexSearcher({
    indexPath: indexPath
  }, function (err, si) {
    t.error(err)
    var q = {
      query: [
        {
          AND: {
            title: ['oppsigelse'],
            text: ['oppsigelse', 'usakelig', 'arbeidsmiljøl']
          }
        }
      ]
    }
    si.search(q).on('data', function (data) {
      t.equals(data.document.id, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

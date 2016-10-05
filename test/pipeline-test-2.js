const SearchIndexAdder = require('../')
// const SearchIndexSearcher = require('search-index-searcher')
const test = require('tape')
// const JSONStream = require('JSONStream')
const Readable = require('stream').Readable

const data = [
  {
    id: 'one',
    title: 'First',
    price: 8,
    text: {
      The: 'first doc'
    }
  },
  {
    id: 'two',
    title: 'Second',
    price: 6,
    text: 'the SeCond doc'
  },
  {
    id: 'three',
    title: 'third',
    price: 7,
    text: 'the third DoC'
  },
  {
    id: 'four',
    title: 'four',
    price: 9,
    text: 'the fourth dOc'
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
    indexPath: 'test/sandbox/deleteTest',
    stopwords: ['the'],
    fieldOptions: {
      title: {
        weight: 2
      },
      price: {
        sortable: true
      }
    }
  }, function (err, si) {
    t.error(err)
    s.pipe(si.defaultPipeline())
    s.pipe(si.add())
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

// do stuff with customising pipelines here


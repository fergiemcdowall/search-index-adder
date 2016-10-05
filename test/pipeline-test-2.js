const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const pumpify = require('pumpify')
const test = require('tape')

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

test('make the search index, removing the pipeline stage that bumps text to lower case', function (t) {
  t.plan(7)
  const s = new Readable()
  data.forEach(function (stone) {
    s.push(JSON.stringify(stone))
  })
  s.push(null)

  SearchIndexAdder({
    indexPath: 'test/sandbox/customPipeline',
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
    s.pipe(JSONStream.parse())
      .pipe(pumpify.obj(
        // the LowerCase stage is removed
        new si.pipeline.IngestDoc(si.options),
        new si.pipeline.CreateStoredDocument(si.options),
        new si.pipeline.NormaliseFields(si.options),
        new si.pipeline.Tokeniser(si.options),
        new si.pipeline.RemoveStopWords(si.options),
        new si.pipeline.CalculateTermFrequency(si.options),
        new si.pipeline.CreateCompositeVector(si.options),
        new si.pipeline.CreateSortVectors(si.options),
        new si.pipeline.FieldedSearch(si.options)
      ))
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

test('confirm can search with document deleted', function (t) {
  t.plan(3)
  SearchIndexSearcher({
    indexPath: 'test/sandbox/customPipeline'
  }, function (err, si) {
    t.error(err)
    si.search({
      query: {
        AND: {'*': ['DoC']}
      }
    }).on('data', function (data) {
      data = JSON.parse(data)
      t.equals(data.document.id, 'three')
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

// do stuff with customising pipelines here


const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const logLevel = process.env.NODE_ENV || 'info'
const sandbox = process.env.SANDBOX || 'test/sandbox'
const test = require('tape')

const getReadStream = function () {
  const batch = [
    {
      id: '1',
      name: 'Apple Watch',
      description: 'Receive and respond to notiﬁcations in an instant.',
      price: '20002',
      age: '346'
    },
    {
      id: '2',
      name: 'Victorinox Swiss Army',
      description: 'You have the power to keep time moving with this Airboss automatic watch.',
      price: '99',
      age: '33342'
    },
    {
      id: '3',
      name: "Versace Men's Swiss",
      description: "Versace Men's Swiss Chronograph Mystique Sport Two-Tone Ion-Plated Stainless Steel Bracelet Watch",
      price: '4716',
      age: '8293'
    },
    {
      id: '4',
      name: "CHARRIOL Men's Swiss Alexandre",
      description: 'With CHARRIOLs signature twisted cables, the Alexander C timepiece collection is a must-have piece for lovers of the famed brand.',
      price: '2132',
      age: '33342'
    },
    {
      id: '5',
      name: "Ferragamo Men's Swiss 1898",
      description: 'The 1898 timepiece collection from Ferragamo offers timeless luxury.',
      price: '99999',
      age: '33342'
    },
    {
      id: '6',
      name: 'Bulova AccuSwiss',
      description: 'The Percheron Treble timepiece from Bulova AccuSwiss sets the bar high with sculpted cases showcasing sporty appeal. A Manchester United® special edition.',
      price: '1313',
      age: '33342'
    },
    {
      id: '7',
      name: 'TW Steel',
      description: 'A standout timepiece that boasts a rich heritage and high-speed design. This CEO Tech watch from TW Steel sets the standard for elite. Armani',
      price: '33333',
      age: '33342'
    },
    {
      id: '8',
      name: 'Invicta Bolt Zeus ',
      description: "Invicta offers an upscale timepiece that's as full of substance as it is style. From the Bolt Zeus collection.",
      price: '8767',
      age: '33342'
    },
    {
      id: '9',
      name: 'Victorinox Night Vision ',
      description: "Never get left in the dark with Victorinox Swiss Army's Night Vision watch. First at Macy's!",
      price: '1000',
      age: '33342'
    },
    {
      id: '10',
      name: 'Armani Swiss Moon Phase',
      description: 'Endlessly sophisticated in materials and design, this Emporio Armani Swiss watch features high-end timekeeping with moon phase movement and calendar tracking swiss swiss.',
      price: '30000',
      age: '33342'
    }
  ]
  const s = new Readable()
  batch.forEach(function (item) {
    s.push(JSON.stringify(item))
  })
  s.push(null)
  return s
}

test('initialize a search index with no fielded search', function (t) {
  t.plan(13)
  SearchIndexAdder({
    fieldedSearch: false,
    indexPath: sandbox + '/si-no-fielded-search',
    logLevel: logLevel
  }, function (err, indexer) {
    t.error(err)
    getReadStream().pipe(JSONStream.parse())
      .pipe(indexer.defaultPipeline())
      // .on('data', function(data) {
      //   console.log(JSON.parse(data, null, 2))
      // })
      .pipe(indexer.add())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        indexer.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('search in a search index with no fielded search', function (t) {
  var counter = 0
  t.plan(4)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-no-fielded-search',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search('moon calendar')
      .on('data', function (data) {
        t.ok(JSON.parse(data).document.id === '10')
        t.equals(++counter, 1)
      }).on('end', function () {
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('search in a search index with no fielded search', function (t) {
  var results = ['5', '4']
  var counter = 0
  t.plan(5)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-no-fielded-search',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search('swiss timepiece')
      .on('data', function (data) {
        counter++
        t.equals(JSON.parse(data).document.id, results.shift())
      }).on('end', function () {
        t.equals(counter, 2)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('cant do a fielded search', function (t) {
  var counter = 0
  t.plan(3)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-no-fielded-search',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {name: ['versace']}
      }
    })
      .on('data', function (data) {
        counter++
      }).on('end', function () {
        t.equals(counter, 0)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('initialize a search index WITH fielded search', function (t) {
  t.plan(13)
  SearchIndexAdder({
    fieldedSearch: true,
    indexPath: sandbox + '/si-fielded-search-on',
    logLevel: logLevel
  }, function (err, indexer) {
    t.error(err)
    getReadStream().pipe(JSONStream.parse())
      .pipe(indexer.defaultPipeline())
      .pipe(indexer.add())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        indexer.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('CAN do a fielded search', function (t) {
  var counter = 0
  t.plan(4)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-fielded-search-on',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {name: ['versace']}
      }
    })
      .on('data', function (data) {
        t.equals(JSON.parse(data).document.id, '3')
        counter++
      }).on('end', function () {
        t.equals(counter, 1)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('CAN do a fielded search', function (t) {
  var results = ['9', '3', '10']
  var counter = 0
  t.plan(6)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-fielded-search-on',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {description: ['swiss', 'watch']}
      }
    })
      .on('data', function (data) {
        t.equals(JSON.parse(data).document.id, results.shift())
        counter++
      }).on('end', function () {
        t.equals(counter, 3)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('initialize a search index WITH fielded search on specified fields', function (t) {
  t.plan(13)
  SearchIndexAdder({
    fieldedSearch: false,
    indexPath: sandbox + '/si-fielded-search-specified-fields',
    logLevel: logLevel
  }, function (err, indexer) {
    t.error(err)
    getReadStream().pipe(JSONStream.parse())
      .pipe(indexer.defaultPipeline({
        fieldOptions: {
          description: {
            fieldedSearch: true
          }
        }
      }))
      .pipe(indexer.add())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        indexer.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('CAN do a fielded search on "description" field', function (t) {
  var results = ['9', '3', '10']
  var counter = 0
  t.plan(6)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-fielded-search-specified-fields',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {description: ['swiss', 'watch']}
      }
    })
      .on('data', function (data) {
        t.equals(JSON.parse(data).document.id, results.shift())
        counter++
      }).on('end', function () {
        t.equals(counter, 3)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('CANT do a fielded search on name field', function (t) {
  var counter = 0
  t.plan(3)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-fielded-search-specified-fields',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {name: ['versace']}
      }
    })
      .on('data', function (data) {
        counter++
      }).on('end', function () {
        t.equals(counter, 0)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('CAN find stuff from name field in wildcard', function (t) {
  var counter = 0
  t.plan(4)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-fielded-search-specified-fields',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {'*': ['versace']}
      }
    })
      .on('data', function (data) {
        t.equals(JSON.parse(data).document.id, '3')
        counter++
      }).on('end', function () {
        t.equals(counter, 1)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('initialize a search index WITH only name field searchable', function (t) {
  t.plan(13)
  SearchIndexAdder({
    searchable: false,
    indexPath: sandbox + '/si-searchable-specified-fields',
    logLevel: logLevel
  }, function (err, indexer) {
    t.error(err)
    getReadStream().pipe(JSONStream.parse())
      .pipe(indexer.defaultPipeline({
        fieldOptions: {
          name: {
            searchable: true
          }
        }
      }))
      .pipe(indexer.add())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        indexer.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('CAN find stuff from name field in wildcard', function (t) {
  var counter = 0
  t.plan(4)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-searchable-specified-fields',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {'*': ['versace']}
      }
    })
      .on('data', function (data) {
        t.looseEquals(JSON.parse(data).document, {
          age: '8293',
          description: 'Versace Men\'s Swiss Chronograph Mystique Sport Two-Tone Ion-Plated Stainless Steel Bracelet Watch',
          id: '3',
          name: 'Versace Men\'s Swiss',
          price: '4716' })
        counter++
      }).on('end', function () {
        t.equals(counter, 1)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('CANT find stuff from description field in wildcard', function (t) {
  var counter = 0
  t.plan(3)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-searchable-specified-fields',
    logLevel: logLevel
  }, function (err, searcher) {
    t.error(err)
    searcher.search({
      query: {
        AND: {'*': ['chronograph', 'mystique', 'sport']}
      }
    })
      .on('data', function (data) {
        counter++
      }).on('end', function () {
        t.equals(counter, 0)
        searcher.close(function (err) {
          t.error(err)
        })
      })
  })
})

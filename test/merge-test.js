const JSONStream = require('JSONStream')
const SearchIndexSearcher = require('search-index-searcher')
const SearchIndexAdder = require('../')
const s = require('stream')
const test = require('tape')
const zlib = require('zlib')

const stones = [
  {
    name: 'Mick',
    band: ['stones'],
    bio: 'Jagger\'s career has spanned over 50 years, and he has been described as "one of the most popular and influential frontmen in the history of Rock & Roll".[7] Jagger\'s distinctive voice and performance, along with Keith Richards\' guitar style, have been the trademark of the Rolling Stones throughout the career of the band'
  },
  {
    name: 'Kieth',
    band: ['stones'],
    bio: 'Keith Richards (born 18 December 1943) is an English guitarist, singer, songwriter, best-selling memoirist, and founding member of the rock band The Rolling Stones. Rolling Stone Magazine credited Richards for "rock\'s greatest single body of riffs" on guitar and ranked him 4th on its list of 100 best guitarists.'
  },
  {
    name: 'Charlie',
    band: ['stones'],
    bio: 'Best known as a member of The Rolling Stones. Originally trained as a graphic artist, he started playing drums in London’s rhythm and blues clubs, where he met Brian Jones, Mick Jagger, and Keith Richards. In 1963, he joined their group, the Rolling Stones, as drummer, while doubling as designer of their record sleeves and tour stages'
  },
  {
    name: 'Ronnie',
    band: ['stones'],
    bio: 'English rock musician, singer, songwriter, artist and radio personality best known as a member of The Rolling Stones since 1975, as well as a member of Faces and the Jeff Beck Group.'
  }
]

const beatles = [
  {
    name: 'John',
    band: ['beatles'],
    bio: 'English singer and songwriter who co-founded the Beatles (1960-70), the most commercially successful band in the history of popular music. With fellow member Paul McCartney, he formed a lucrative songwriting partnership'
  },
  {
    name: 'Paul',
    band: ['beatles'],
    bio: 'English singer-songwriter, multi-instrumentalist, and composer. With John Lennon, George Harrison, and Ringo Starr, he gained worldwide fame with the rock band the Beatles, one of the most popular and influential groups in the history of pop music.'
  },
  {
    name: 'George',
    band: ['beatles'],
    bio: 'Often referred to as "the quiet Beatle",[3][4] Harrison embraced Indian mysticism and helped broaden the horizons of his fellow Beatles as well as their Western audience by incorporating Indian instrumentation in their music'
  },
  {
    name: 'Ringo',
    band: ['beatles'],
    bio: 'English musician, singer, songwriter and actor who gained worldwide fame as the drummer for the Beatles. He occasionally sang lead vocals, usually for one song on an album, including "With a Little Help from My Friends", "Yellow Submarine" and their cover of "Act Naturally"'
  }
]

const beatlesStream = new s.Readable()
beatles.forEach(function (beatle) {
  beatlesStream.push(JSON.stringify(beatle))
})
beatlesStream.push(null)

const stonesStream = new s.Readable()
stones.forEach(function (stone) {
  stonesStream.push(JSON.stringify(stone))
})
stonesStream.push(null)

test('make the beatles search index', function (t) {
  t.plan(7)
  SearchIndexAdder({
    indexPath: 'test/sandbox/beatles'
  }, function (err, si) {
    t.error(err)
    beatlesStream
      .pipe(JSONStream.parse())
      .pipe(si.createWriteStream())
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

test('make the stones search index', function (t) {
  t.plan(7)
  SearchIndexAdder({
    indexPath: 'test/sandbox/stones'
  }, function (err, si) {
    t.error(err)
    stonesStream
      .pipe(JSONStream.parse())
      .pipe(si.createWriteStream())
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

test('confirm can search as normal in beatles', function (t) {
  t.plan(6)
  var results = [ 'Ringo', 'George', 'Paul', 'John' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/beatles'
  }, function (err, si) {
    t.error(err)
    si.search({
      AND: [{'*': ['*']}]
    }).on('data', function (data) {
      data = JSON.parse(data)
      t.ok(data.document.name, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('confirm can search as normal in stones', function (t) {
  t.plan(6)
  var results = [ 'Ronnie', 'Charlie', 'Kieth', 'Mick' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/stones'
  }, function (err, si) {
    t.error(err)
    si.search({
      AND: [{'*': ['*']}]
    }).on('data', function (data) {
      data = JSON.parse(data)
      t.ok(data.document.name, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('gzipped replication from beatles to supergroup', function (t) {
  t.plan(4)
  SearchIndexAdder({
    indexPath: 'test/sandbox/supergroup'
  }, function (err, supergroup) {
    t.error(err)
    SearchIndexSearcher({
      indexPath: 'test/sandbox/beatles'
    }, function (err, beatles) {
      t.error(err)
      beatles.dbReadStream({gzip: true})
        .pipe(zlib.createGunzip())
        .pipe(JSONStream.parse())
        .pipe(supergroup.dbWriteStream())
        .on('data', function () {})
        .on('end', function () {
          supergroup.close(function (err) {
            t.error(err)
            beatles.close(function (err) {
              t.error(err)
            })
          })
        })
    })
  })
})

test('supergroup contains beatles', function (t) {
  t.plan(6)
  var results = [ 'Ringo', 'George', 'Paul', 'John' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/supergroup'
  }, function (err, si) {
    t.error(err)
    si.search({
      AND: [{'*': ['*']}]
    }).on('data', function (data) {
      data = JSON.parse(data)
      t.ok(data.document.name, results.shift())
    }).on('end', function () {
      si.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('gzipped replication of stones to supergroup', function (t) {
  t.plan(4)
  SearchIndexAdder({
    indexPath: 'test/sandbox/supergroup'
  }, function (err, supergroup) {
    t.error(err)
    SearchIndexSearcher({
      indexPath: 'test/sandbox/stones'
    }, function (err, stones) {
      t.error(err)
      stones.dbReadStream({gzip: true})
        .pipe(zlib.createGunzip())
        .pipe(JSONStream.parse())
        .pipe(supergroup.dbWriteStream())
        .on('data', function () {})
        .on('end', function () {
          supergroup.close(function (err) {
            t.error(err)
            stones.close(function (err) {
              t.error(err)
            })
          })
        })
    })
  })
})

test('open supergroup index', function (t) {
  t.plan(9)
  var results = [ 'Ronnie', 'Ringo', 'George', 'Charlie', 'Paul', 'Kieth', 'Mick', 'John' ]
  SearchIndexSearcher({
    indexPath: 'test/sandbox/supergroup'
  }, function (err, si) {
    t.error(err)
    si.search({
      query: [{
        AND: {'*': ['*']}
      }]
    }).on('data', function (data) {
      data = JSON.parse(data)
      t.ok(data.document.name === results.shift())
    })
  })
})

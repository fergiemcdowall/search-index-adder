const JSONStream = require('JSONStream')
const SearchIndex = require('search-index')
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

var supergroup, beatlesIndex, stonesIndex

test('make the beatles search index', function (t) {
  t.plan(7)
  SearchIndex({
    indexPath: 'test/sandbox/beatles'
  }, function (err, si) {
    t.error(err)
    beatlesIndex = si
    beatlesStream
      .pipe(JSONStream.parse())
      .pipe(beatlesIndex.createWriteStream())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        t.ok(true, ' stream ended')
      })
  })
})

test('make the stones search index', function (t) {
  t.plan(7)
  SearchIndex({
    indexPath: 'test/sandbox/stones'
  }, function (err, si) {
    t.error(err)
    stonesIndex = si
    stonesStream
      .pipe(JSONStream.parse())
      .pipe(stonesIndex.createWriteStream())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        t.ok(true, ' stream ended')
      })
  })
})

test('confirm can search as normal in beatles', function (t) {
  t.plan(2)
  beatlesIndex.search({
    query: {
      AND: [{'*': ['*']}]
    }
  }, function (err, results) {
    t.error(err)
    t.looseEqual(
      results.hits.map(function (item) {
        return item.document.name
      }),
      [ 'Ringo', 'George', 'Paul', 'John' ]
    )
  })
})

test('confirm can search as normal in stones', function (t) {
  t.plan(2)
  stonesIndex.search({
    query: {
      AND: [{'*': ['*']}]
    }
  }, function (err, results) {
    t.error(err)
    t.looseEqual(
      results.hits.map(function (item) {
        return item.document.name
      }),
      [ 'Ronnie', 'Charlie', 'Kieth', 'Mick' ]
    )
  })
})

test('make supergroup index', function (t) {
  t.plan(1)
  SearchIndex({
    indexPath: 'test/sandbox/supergroup'
  }, function (err, si) {
    t.error(err)
    supergroup = si
  })
})

test('gzipped replication from beatles to supergroup', function (t) {
  t.plan(1)
  beatlesIndex.DBReadStream({gzip: true})
    .pipe(zlib.createGunzip())
    .pipe(JSONStream.parse())
    .pipe(supergroup.DBWriteStream())
    .on('close', function () {
      t.ok(true, 'stream closed')
    })
})

test('supergroup contains beatles', function (t) {
  t.plan(2)
  supergroup.search({
    query: {
      AND: [{'*': ['*']}]
    }
  }, function (err, results) {
    t.error(err)
    t.looseEqual(
      results.hits.map(function (item) {
        return item.document.name
      }),
      [ 'Ringo', 'George', 'Paul', 'John' ]
    )
  })
})

test('close indexes', function (t) {
  t.plan(3)
  // close all search indexes and reopen them as replicators
  beatlesIndex.close(function (err) {
    t.error(err)
  })
  stonesIndex.close(function (err) {
    t.error(err)
  })
  supergroup.close(function (err) {
    t.error(err)
  })
})

test('open replicators', function (t) {
  t.plan(3)
  Replicator({
    indexPath: 'test/sandbox/beatles'
  }, function (err, si) {
    t.error(err)
    beatlesIndex = si
  })
  Replicator({
    indexPath: 'test/sandbox/stones'
  }, function (err, si) {
    t.error(err)
    stonesIndex = si
  })
  Replicator({
    indexPath: 'test/sandbox/supergroup'
  }, function (err, si) {
    t.error(err)
    supergroup = si
  })
})

test('gzipped merge of stones into supergroup', function (t) {
  t.plan(1)
  stonesIndex.DBReadStream({gzip: true})
    .pipe(zlib.createGunzip())
    .pipe(JSONStream.parse())
    .pipe(supergroup.DBWriteStream({merge: true}))
    .on('data', function (data) {
      console.log('data')
    })
    .on('end', function () {
      console.log('stream ended')
      t.ok(true, 'stream ended')
    })
})

test('close replicators', function (t) {
  t.plan(3)
  // close all search indexes and reopen them as replicators
  beatlesIndex.close(function (err) {
    t.error(err)
  })
  stonesIndex.close(function (err) {
    t.error(err)
  })
  supergroup.close(function (err) {
    t.error(err)
  })
})

test('open supergroup index', function (t) {
  t.plan(3)
  SearchIndex({
    indexPath: 'test/sandbox/supergroup'
  }, function (err, si) {
    t.error(err)
    supergroup = si
    supergroup.search({
      query: {
        AND: [{'*': ['*']}]
      }
    }, function (err, results) {
      t.error(err)
      t.looseEqual(
        results.hits.map(function (item) {
          return item.document.name
        }),
        [ 'Ronnie', 'Ringo', 'George', 'Charlie', 'Paul', 'Kieth', 'Mick', 'John' ]
      )
    })
  })
})

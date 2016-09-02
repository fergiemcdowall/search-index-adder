const JSONStream = require('JSONStream')
const SearchIndexAdder = require('../')
const SearchIndexSearcher = require('search-index-searcher')
const fs = require('fs')
const sandbox = process.env.SANDBOX || 'test/sandbox'
const test = require('tape')
const zlib = require('zlib')

const indexAddress = sandbox + '/replicate-test'

var replicator, replicatorTarget, replicatorTarget2

test('make a small search index', function (t) {
  t.plan(14)
  SearchIndexAdder({
    indexPath: indexAddress
  }, function (err, si) {
    t.error(err)
    const filePath = './node_modules/reuters-21578-json/data/fullFileStream/justTen.str'
    fs.createReadStream(filePath)
      .pipe(JSONStream.parse())
      .pipe(si.defaultPipeline())
      .pipe(si.createWriteStream2())
      .on('data', function (data) {
        t.ok(true, ' data recieved')
      })
      .on('end', function () {
        t.ok(true, ' stream ended')
        si.close(function (err) {
          t.error(err, ' index closed')
        })
      })
  })
})

test('initialise', function (t) {
  t.plan(1)
  SearchIndexSearcher({indexPath: sandbox + '/replicate-test'}, function (err, thisReplicator) {
    t.error(err)
    replicator = thisReplicator
  })
})

test('simple read from replicator (no ops)', function (t) {
  t.plan(1)
  var i = 0
  replicator.dbReadStream()
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 3107)
    })
})

test('simple read from replicator (gzip: false)', function (t) {
  t.plan(1)
  var i = 0
  replicator.dbReadStream({gzip: false})
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 3107)
    })
})

test('simple read from replicator (gzip: true)', function (t) {
  t.plan(1)
  var i = 0
  replicator.dbReadStream({gzip: true})
    .pipe(zlib.createGunzip())
    .pipe(JSONStream.parse())
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 3107)
    })
})

test('initialise replication target', function (t) {
  t.plan(1)
  SearchIndexAdder({indexPath: sandbox + '/replicate-test-target'}, function (err, thisReplicator) {
    t.error(err)
    replicatorTarget = thisReplicator
  })
})

test('simple replication from one index to another', function (t) {
  t.plan(5)
  replicator.dbReadStream()
    .pipe(replicatorTarget.dbWriteStream({ merge: false }))
    .on('data', function (data) {
      t.ok(true, 'data event received')
    })
    .on('end', function () {
      replicatorTarget.close(function (err) {
        t.error(err)
      })
    })
})

test('initialise replication target', function (t) {
  t.plan(1)
  SearchIndexSearcher({indexPath: sandbox + '/replicate-test-target'}, function (err, thisReplicator) {
    t.error(err)
    replicatorTarget = thisReplicator
  })
})

test('simple read from replicated index (no ops)', function (t) {
  t.plan(1)
  var i = 0
  replicatorTarget.dbReadStream()
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 3107)
    })
})

test('initialise replication target2', function (t) {
  t.plan(1)
  SearchIndexAdder({indexPath: sandbox + '/replicate-test-target2'}, function (err, thisReplicator) {
    t.error(err)
    replicatorTarget2 = thisReplicator
  })
})

test('gzipped replication from one index to another', function (t) {
  t.plan(1)
  replicator.dbReadStream({gzip: true})
    .pipe(zlib.createGunzip())
    .pipe(JSONStream.parse())
    .pipe(replicatorTarget2.dbWriteStream())
    .on('data', function (data) {
      console.log('BOOOOOOOOOOM')
    // data
    })
    .on('error', function (err) {
      console.log(err)
    })
    .on('end', function () {
      replicatorTarget2.close(function (err) {
        t.error(err)
      })
    })
})

test('initialise replication target2', function (t) {
  t.plan(1)
  SearchIndexSearcher({indexPath: sandbox + '/replicate-test-target2'}, function (err, thisReplicator) {
    t.error(err)
    replicatorTarget2 = thisReplicator
  })
})

test('validate gzip replication', function (t) {
  t.plan(1)
  var i = 0
  replicatorTarget2.dbReadStream()
    .on('data', function (data) {
      i++
    })
    .on('end', function () {
      t.equal(i, 3107)
    })
})

test('confirm can search as normal in replicated index', function (t) {
  t.plan(10)
  var results = [ '9', '8', '7', '6', '5', '4', '3', '2', '10', '1' ]
  replicatorTarget2.search({
    query: [{
      AND: {'*': ['*']}
    }]
  }).on('data', function (data) {
    data = JSON.parse(data)
    t.ok(results.shift() === data.document.id)
  })
})

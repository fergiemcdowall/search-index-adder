const DBEntries = require('./delete.js').DBEntries
const DBWriteCleanStream = require('./replicate.js').DBWriteCleanStream
const DBWriteMergeStream = require('./replicate.js').DBWriteMergeStream
const DocVector = require('./delete.js').DocVector
const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const RecalibrateDB = require('./delete.js').RecalibrateDB
const IndexBatch = require('./add.js').IndexBatch
const async = require('async')
const del = require('./delete.js')
const docProc = require('docproc')
const pumpify = require('pumpify')

module.exports = function (options) {
  const q = async.queue(function (batch, done) {
    var s
    if (batch.operation === 'add') {
      s = new Readable({ objectMode: true })
      batch.batch.forEach(function (doc) {
        s.push(doc)
      })
      s.push(null)
      s.pipe(Indexer.defaultPipeline(batch.batchOps))
        .pipe(Indexer.add())
        .on('finish', function () {
          return done()
        })
        .on('error', function (err) {
          return done(err)
        })
    } else if (batch.operation === 'delete') {
      s = new Readable()
      batch.docIds.forEach(function (docId) {
        s.push(JSON.stringify(docId))
      })
      s.push(null)
      s.pipe(Indexer.deleteStream(options))
        .on('data', function () {
          // nowt
        })
        .on('end', function () {
          done(null)
        })
    }
  }, 1)

  const add = function (ops) {
    // override options with every add
    const thisOps = Object.assign(options, ops)
    return pumpify.obj(
      new IndexBatch(deleter, thisOps),
      new DBWriteMergeStream(thisOps)
    )
  }

  const concurrentAdd = function (batchOps, batch, done) {
    q.push({
      batch: batch,
      batchOps: batchOps,
      operation: 'add'
    }, function (err) {
      done(err)
    })
  }

  const concurrentDel = function (docIds, done) {
    q.push({
      docIds: docIds,
      operation: 'delete'
    }, function (err) {
      done(err)
    })
  }

  const close = function (callback) {
    options.indexes.close(function (err) {
      while (!options.indexes.isClosed()) {
        options.log.debug('closing...')
      }
      if (options.indexes.isClosed()) {
        options.log.debug('closed...')
        callback(err)
      }
    })
  }

  const dbWriteStream = function (streamOps) {
    streamOps = Object.assign({}, { merge: true }, streamOps)
    if (streamOps.merge) {
      return new DBWriteMergeStream(options)
    } else {
      return new DBWriteCleanStream(options)
    }
  }

  const defaultPipeline = function (batchOptions) {
    batchOptions = Object.assign({}, options, batchOptions)
    return docProc.pipeline(batchOptions)
  }

  const deleteStream = function (options) {
    return pumpify.obj(
      new DocVector(options),
      new DBEntries(options),
      new RecalibrateDB(options)
    )
  }

  const deleter = function (docIds, done) {
    const s = new Readable()
    docIds.forEach(function (docId) {
      s.push(JSON.stringify(docId))
    })
    s.push(null)
    s.pipe(Indexer.deleteStream(options))
      .on('data', function () {
        // nowt
      })
      .on('end', function () {
        done(null)
      })
  }

  const feed = function (ops) {
    if (ops && ops.objectMode) {
      // feed from stream of objects
      return pumpify.obj(
        Indexer.defaultPipeline(ops),
        Indexer.add(ops)
      )
    } else {
      // feed from stream of strings
      return pumpify(
        JSONStream.parse(),
        Indexer.defaultPipeline(ops),
        Indexer.add(ops)
      )
    }
  }

  const flush = function (APICallback) {
    del.flush(options, function (err) {
      return APICallback(err)
    })
  }

  const Indexer = {}
  Indexer.add = add
  Indexer.concurrentAdd = concurrentAdd
  Indexer.concurrentDel = concurrentDel
  Indexer.close = close
  Indexer.dbWriteStream = dbWriteStream
  Indexer.defaultPipeline = defaultPipeline
  Indexer.deleteStream = deleteStream
  Indexer.deleter = deleter
  Indexer.feed = feed
  Indexer.flush = flush
  return Indexer
}

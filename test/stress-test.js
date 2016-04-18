var fs = require('fs')
var sia = require('../')
var indexPath = 'test/sandbox/stressTest'

require('search-index')({indexPath: indexPath}, function(err, si) {
  console.log('index initialised')
  si.replicate(fs.createReadStream('test/stress-test-backup.gz'), function(err) {
    console.log('index replicated')
    var batch = require('./breakyBatch.json')
    si.add(batch, {}, function(err) {
      console.log('batch added')
      if (err)
        console.log('error indexing ' + batchPath + ' : ' + err)
      else
        console.log('batch indexed')
      return
    })
  })
})



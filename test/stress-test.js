// provokes an "out of memory" error when run with
// node --max_old_space_size=200

var indexPath = 'test/sandbox/stressTest'

require('search-index')({indexPath: indexPath}, function (err, si) {
  if (err) console.log('oops')
  console.log('index initialised')
  // si.replicate(fs.createReadStream('test/stress-test-backup.gz'), function (err) {
  //   if (err) console.log('oops')
  //   console.log('index replicated')
  var batch = require('./breakyBatch.json')
  si.close(function (err) {
    if (err) console.log('oops')
    require('../')({indexPath: indexPath}, function (err, sia) {
      if (err) console.log('oops')
      sia.add(batch, {}, function (err) {
        console.log('batch added')
        if (err) {
          console.log('error indexing ' + err)
        } else {
          console.log('batch indexed')
        }
        return
      })
    })
  })
// })
})

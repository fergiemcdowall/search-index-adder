const util = require('util')
const Transform = require('stream').Transform

const IngestDoc = function (options) {
  this.options = options
  this.i = 0
  Transform.call(this, { objectMode: true })
}
exports.IngestDoc = IngestDoc
util.inherits(IngestDoc, Transform)
IngestDoc.prototype._transform = function (doc, encoding, end) {
  var ingestedDoc = {
    id: String(String(doc.id) || (Date.now() + '-' + ++this.i)),
    vector: {},
    stored: {},
    raw: doc,
    normalised: doc
  }
  this.push(JSON.stringify(ingestedDoc))
  return end()
}


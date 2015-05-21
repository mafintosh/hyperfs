#!/usr/bin/env node

var minimist = require('minimist')
var mkdirp = require('mkdirp')
var argv = minimist(process.argv.slice(2))
var s = argv.store || 'cauf'

mkdirp.sync(s)

var cauf = require('./')(s)

var cmd = argv._[0]

if (cmd === 'list' || cmd === 'ls') {
  cauf.list().on('data', console.log)
  return
}

if (cmd === 'list-snapshots' || cmd === 'ls-snapshots') {
  cauf.listSnapshots().on('data', console.log)
  return
}

if (cmd === 'remove' || cmd === 'rm') {
  cauf.remove(argv._[1], function () {
    console.log(argv._[1] + ' removed')
  })
  return
}

if (cmd === 'info') {
  cauf.info(argv._[1], console.log)
  return
}

if (cmd === 'create') {
  cauf.create(argv._[1], argv, console.log)
  return
}

if (cmd === 'mount') {
  var mnt = cauf.mount(argv._[1], argv._[2], argv)
  mnt.on('ready', function () {
    console.log(mnt.id, 'mounted')
    ;[].concat(mnt.layers).reverse().slice(1).forEach(function (l) {
      console.log('<-- ' + l)
    })
  })
  process.on('SIGINT', function () {
    cauf.unmount('mnt', function () {
      process.exit()
    })
  })
  return
}

if (cmd === 'snapshot') {
  var snapshot = cauf.snapshot(argv._[1])
  snapshot.on('index', function (name) {
    console.log('Indexing ' + name)
  })
  snapshot.on('snapshot', function (name) {
    console.log('Snapshotting ' + name)
  })
  snapshot.on('finish', function () {
    console.log('Snapshot complete: ' + snapshot.key)
  })
  return
}

console.log('Unknown command')
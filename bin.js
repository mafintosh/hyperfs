#!/usr/bin/env node

var minimist = require('minimist')
var mkdirp = require('mkdirp')
var transports = require('transport-stream')({command: 'cauf replicate -'})
var argv = minimist(process.argv.slice(2), {alias: {message: 'm', quiet: 'q', debug: 'D'}})
var s = argv.store || 'cauf'

mkdirp.sync(s)

var cauf = require('./')(s)

var cmd = argv._[0]

if (cmd === 'list' || cmd === 'ls') {
  cauf.list().on('data', console.log)
  return
}

if (cmd === 'nodes') {
  cauf.nodes().on('data', function (data) {
    console.log(data.key+': ' + (data.value.message || '(no message)'))
  })
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

if (cmd === 'replicate') {
  var stream = transports(argv._[1])
  var rs = cauf.replicate(argv)

  stream.on('warn', console.error)

  if (argv._[1] !== '-' && !argv.quiet) {
    rs.on('receive-data', function (data) {
      console.error('receive-data: ' + data)
    })

    rs.on('receive-snapshot', function (hash) {
      console.error('receive-snapshot: ' + hash)
    })

    rs.on('send-data', function (data) {
      console.error('send-data: ' + data)
    })

    rs.on('send-snapshot', function (hash) {
      console.error('send-snapshot: ' + hash)
    })
  }

  stream.pipe(rs).pipe(stream)
  return
}

if (cmd === 'mount') {
  var mnt = cauf.mount(argv._[1], argv._[2], argv)
  mnt.on('ready', function () {
    console.log(mnt.id, 'mounted')
    ;[].concat(mnt.nodes).reverse().forEach(function (l) {
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
  var snapshot = cauf.snapshot(argv._[1], argv)
  snapshot.on('index', function (name) {
    console.log('Indexing ' + name)
  })
  snapshot.on('snapshot', function (name) {
    console.log('Snapshotting ' + name)
  })
  snapshot.on('finish', function () {
    console.log('Snapshot complete: ' + snapshot.node)
  })
  return
}

console.log('Unknown command')
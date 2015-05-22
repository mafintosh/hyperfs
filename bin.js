#!/usr/bin/env node

var fs = require('fs')
var minimist = require('minimist')
var mkdirp = require('mkdirp')
var path = require('path')
var transports = require('transport-stream')({command: 'hyperfs replicate -'})
var execspawn = require('execspawn')
var argv = minimist(process.argv.slice(2), {alias: {message: 'm', quiet: 'q', debug: 'D', store: 's', node: 'n'}})
var s = argv.store || 'hyperfs'

mkdirp.sync(s)

var cauf = require('./')(s)

var cmd = argv._[0]

if (cmd === 'list' || cmd === 'ls') {
  cauf.list().on('data', console.log)
  return
}

if (cmd === 'show') {
  cauf.show(argv._[1], function (err, val) {
    if (err) throw err
    cauf.readSnapshot(val.snapshot, function (err, rs) {
      if (err) throw err
      rs.on('data', function (data) {
        console.log(JSON.stringify(data))
      })
    })
  })
  return
}

if (cmd === 'version') {
  console.log(require('./package.json').version)
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
  if (!argv._[1]) throw new Error('volume required')
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

if (cmd === 'exec') {
  if (!argv._[1]) throw new Error('volume required')
  var folder = argv.mnt || path.join(s, 'mnt', argv._[1])

  mkdirp(folder, function (err) {
    if (err) throw err
    var mnt = cauf.mount(argv._[1], folder, argv)

    mnt.on('ready', function () {
      var proc = execspawn(argv._[2], {
        cwd: folder
      })

      proc.stdout.pipe(process.stdout)
      proc.stderr.pipe(process.stderr)

      proc.on('exit', function (code) {
        cauf.unmount(folder, function () {
          process.exit(code)
        })
      })
    })

    process.on('SIGINT', function () {
      cauf.unmount(folder, function () {
        process.exit()
      })
    })
  })
  return
}

if (cmd === 'mount') {
  if (!argv._[1]) throw new Error('volume required')
  var mnt = cauf.mount(argv._[1], argv._[2] || 'mnt', argv)
  mnt.on('ready', function () {
    console.log(mnt.id, 'mounted')
    ;[].concat(mnt.nodes).reverse().forEach(function (l) {
      console.log('<-- ' + l)
    })
  })
  process.on('SIGINT', function () {
    cauf.unmount(argv._[2] || 'mnt', function () {
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

console.log(fs.readFileSync(__dirname + '/help.txt', 'utf-8'))
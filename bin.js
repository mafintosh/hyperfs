#!/usr/bin/env node

var fs = require('fs')
var minimist = require('minimist')
var mkdirp = require('mkdirp')
var path = require('path')
var transports = require('transport-stream')({command: 'hyperfs replicate -'})
var execspawn = require('execspawn')
var proc = require('child_process')
var pretty = require('pretty-bytes')
var log = require('single-line-log').stderr
var argv = minimist(process.argv.slice(2), {alias: {message: 'm', quiet: 'q', debug: 'D', store: 's', node: 'n'}})
var s = argv.store || 'hyperfs'

mkdirp.sync(s)

var hyperfs = require('./')(s)

var cmd = argv._[0]

if (cmd === 'list' || cmd === 'ls') {
  hyperfs.list().on('data', console.log)
  return
}

if (cmd === 'show') {
  hyperfs.show(argv._[1], function (err, val) {
    if (err) throw err
    hyperfs.readSnapshot(val.snapshot, function (err, rs) {
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
  hyperfs.nodes().on('data', function (data) {
    console.log(data.key+': ' + (data.value.message || '(no message)'))
  })
  return
}

if (cmd === 'remove' || cmd === 'rm') {
  hyperfs.remove(argv._[1], function () {
    console.log(argv._[1] + ' removed')
  })
  return
}

if (cmd === 'info') {
  hyperfs.info(argv._[1], console.log)
  return
}

if (cmd === 'create') {
  if (!argv._[1]) throw new Error('volume required')
  hyperfs.create(argv._[1], argv, function (err) {
    if (err) throw err
  })
  return
}

if (cmd === 'replicate') {
  var stream = transports(argv._[1])
  var rs = hyperfs.replicate(argv)
  var lastUpdate = 0

  stream.on('warn', console.error)

  if (argv._[1] !== '-' && !argv.quiet && process.stdin.isTTY) {
    var read = 0
    var written = 0
    var msg = []

    var print = function () {
      var str = 'Downloaded ' + pretty(read) + ' and uploaded ' + pretty(written) + '\n'
      for (var i = 0; i < Math.min(10, msg.length); i++) {
        str += msg[i] + '\n'
      }
      if (msg.length > 10) msg.shift()
      log(str)
    }

    var printMaybe = function () {
      var time = Date.now()
      if (time - lastUpdate > 200) {
        lastUpdate = time
        print()
      }
    }

    rs.on('read', function (len) {
      read += len
      printMaybe()
    })

    rs.on('write', function (len) {
      written += len
      printMaybe()
    })

    rs.on('receive-data', function (data) {
      msg.push('- receive-data: ' + data)
      print()
    })

    rs.on('receive-snapshot', function (hash) {
      msg.push('- receive-snapshot: ' + hash)
      print()
    })

    rs.on('send-data', function (data) {
      msg.push('- send-data: ' + data)
      print()
    })

    rs.on('send-snapshot', function (hash) {
      msg.push('- send-snapshot: ' + hash)
      print()
    })
  }

  stream.pipe(rs).pipe(stream)
  return
}

if (cmd === 'exec') {
  if (!argv._[1]) throw new Error('volume required')
  var folder = argv._[1]

  mkdirp(folder, function () {
    var mnt = hyperfs.mount(argv._[1], folder, argv)
    var isTTY = process.stdin.isTTY
    mnt.on('ready', function () {
      var child = proc.spawn('linux', [].concat(isTTY ? ['--tty'] : [], ['run', 'sudo', 'chroot', folder, argv._[2]]), {
        stdio: 'inherit'
      })

      child.on('exit', function (code) {
        hyperfs.unmount(folder, function () {
          process.exit(code)
        })
      })
    })

    process.on('SIGINT', function () {
      hyperfs.unmount(folder, function () {
        process.exit()
      })
    })
  })
  return
}

if (cmd === 'mount') {
  if (!argv._[1]) throw new Error('volume required')
  var mnt = hyperfs.mount(argv._[1], argv._[2] || 'mnt', argv)
  mnt.on('ready', function () {
    console.log(mnt.id, 'mounted')
    ;[].concat(mnt.nodes).reverse().forEach(function (l) {
      console.log('<-- ' + l)
    })
  })
  process.on('SIGINT', function () {
    hyperfs.unmount(argv._[2] || 'mnt', function () {
      process.exit()
    })
  })
  return
}

if (cmd === 'snapshot') {
  hyperfs.snapshot(argv._[1], argv, function (err, key) {
    if (err) throw err
    console.error(key)
  })
  return
}

console.log(fs.readFileSync(__dirname + '/help.txt', 'utf-8'))
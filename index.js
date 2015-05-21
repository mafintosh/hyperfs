var path = require('path')
var level = require('level')
var crypto = require('crypto')
var mkdirp = require('mkdirp')
var pumpify = require('pumpify')
var pump = require('pump')
var fs = require('fs')
var fuse = require('fuse-bindings')
var lexint = require('lexicographic-integer')
var union = require('sorted-union-stream')
var events = require('events')
var mknod = require('mknod')
var through = require('through2')
var subleveldown = require('subleveldown')
var enumerate = require('level-enumerate')
var from = require('from2')
var hyperlog = require('hyperlog')
var multiplex = require('multiplex')
var parallel = require('parallel-transform')
var os = require('os')

var noop = function () {}
var ENOENT = new Error('ENOENT')
ENOENT.code = 'ENOENT'

module.exports = function (home) {
  var cauf = {}
  var db = level(path.join(home, 'db'))

  var metadata = subleveldown(db, 'metadata', {valueEncoding: 'json'})
  var inodes = subleveldown(db, 'inodes', {valueEncoding: 'json'})
  var snapshots = subleveldown(db, 'snapshots')
  var ancestors = subleveldown(db, 'ancestors')
  var volumes = subleveldown(db, 'volumes', {valueEncoding: 'json'})
  var log = hyperlog(subleveldown(db, 'log'))

  var writeablePath = function () {
    var name = crypto.randomBytes(32).toString('hex')
    return path.join('writeable', name.slice(0, 2), name.slice(2, 4), name.slice(4))
  }

  var readablePath = function (hash) {
    return path.join('readable', hash.slice(0, 2), hash.slice(2, 4), hash.slice(4))
  }

  var toIndexKey = function(name) {
    var depth = name.split('/').length - 1
    return lexint.pack(depth, 'hex') + '!' + name
  }

  cauf.put = function (id, name, data, cb) {
    var key = id + '!' + toIndexKey(name)
    if (!data.ctime) data.ctime = Date.now()
    if (!data.mtime) data.mtime = Date.now()
    metadata.put(key, data, cb)
  }

  cauf.del = function (id, name, cb) {
    var key = id + '!' + toIndexKey(name)
    metadata.del(key, cb)
  }

  cauf.get = function (id, name, cb) {
    var key = id + '!' + toIndexKey(name)
    metadata.get(key, cb)
  }

  cauf.unmount = function (mnt, cb) {
    fuse.unmount(mnt, cb)
  }

  var dirStream = function (layer, key) {
    return metadata.createReadStream({
      gt: layer + '!' + key,
      lt: layer + '!' + key + '\xff'
    })
  }

  var getInode = function (layer, ino, cb) {
    inodes.get(layer + '!' + lexint.pack(ino, 'hex'), cb)
  }

  var putInode = function (layer, ino, data, cb) {
    inodes.put(layer + '!' + lexint.pack(ino, 'hex'), data, cb)
  }

  var delInode = function (layer, ino, cb) {
    inodes.del(layer + '!' + lexint.pack(ino, 'hex'), cb)
  }

  var countInodes = function (layer, cb) {
    var rs = inodes.createKeyStream({
      gt: layer + '!',
      lt: layer + '!\xff',
      limit: 1,
      reverse: true
    })

    var cnt = 0

    rs.on('data', function (data) {
      cnt = lexint.unpack(data.split('!')[1], 'hex')
    })

    rs.on('error', function (err) {
      cb(err)
    })

    rs.on('end', function () {
      cb(null, cnt)
    })
  }

  var toCompareKey = function (data) {
    return data.key.slice(data.key.indexOf('!') + 1)
  }

  cauf.hasBlob = function (hash, cb) {
    fs.stat(path.join(home, readablePath(hash)), function (err) {
      cb(null, !err)
    })
  }

  cauf.createBlobReadStream = function (key) {
    return fs.createReadStream(path.join(home, readablePath(key)))
  }

  cauf.createBlobWriteStream = function (cb) {
    var filename = path.join(os.tmpdir(), 'cauf-tmp-' + crypto.randomBytes(32).toString('hex'))
    var hash = crypto.createHash('sha256')

    var write = function (data, enc, cb) {
      hash.update(data)
      cb(null, data)
    }

    var ws = fs.createWriteStream(filename)
    var hasher = through(write)

    pump(hasher, ws, function (err) {
      var key = hash.digest('hex')
      var newFilename = path.join(home, readablePath(key))

      mkdirp(path.join(newFilename, '..'), function (err) {
        if (err) return cb(err)
        fs.rename(filename, newFilename, function (err) {
          if (err) return cb(err)
          cb(null, key)
        })
      })
    })

    return hasher
  }

  cauf.readSnapshot = function (key, cb) {
    snapshots.get(key, function (err, space) {
      if (err) return cb(err)

      var rs = snapshots.createValueStream({
        gt: space + '!',
        lt: space + '!\xff',
        valueEncoding: 'json'
      })

      cb(null, rs)
    })
  }

  cauf.snapshot = function (id, opts) { // don't mutate the layer while running this for now
    var monitor = new events.EventEmitter()
    var message = opts.message

    monitor.node = null

    var onindex = function (v) {
      var key = monitor.key = v.snapshot

      if (!v.snapshot) return monitor.emit('finish')

      volumes.put(id, v, function (err) {
        if (err) return monitor.emit('error', err)

        var write = function (data, enc, cb) {
          cauf.put(key, data.name, {special: data.special, deleted: data.deleted, mode: data.mode, uid: data.uid, gid: data.gid, ino: data.ino, rdev: data.rdev}, function (err) {
            if (err) return cb(err)
            cauf.del(id, data.name, function () {
              getInode(id, data.ino || 0, function (err, inode) {
                if (err && err.notFound) return cb() // we already processed this one
                if (err) return cb(err)

                monitor.emit('snapshot', data.name)

                if (!data.data || data.special) {
                  putInode(key, data.ino, inode, function (err) {
                    if (err) return cb(err)
                    delInode(id, data.ino, cb)
                  })
                  return
                }

                var filename = readablePath(data.data)
                mkdirp(path.join(home, filename, '..'), function (err) {
                  if (err) return cb(err)
                  fs.rename(path.join(home, inode.data), path.join(home, filename), function () { // ignore errors for now to be resumeable
                    inode.data = filename
                    putInode(key, data.ino, inode, function (err) {
                      if (err) return cb(err)
                      delInode(id, data.ino, cb)
                    })
                  })
                })
              })
            })
          })
        }

        cauf.readSnapshot(key, function (err, rs) {
          if (err) return monitor.emit('error', err)

          pump(rs, through.obj(write), function () {
            var done = function (err) {
              if (err) return monitor.emit('error', err)
              volumes.put(id, v, function (err) {
                if (err) return monitor.emit('error', err)
                monitor.emit('finish')
              })
            }

            var node = {
              snapshot: key,
              message: message || ''
            }

            log.add(v.node ? [v.node] : [], JSON.stringify(node), function (err, node) {
              if (err) return monitor.emit('error', err)

              v.node = node.key
              v.snapshot = null
              monitor.node = v.node

              volumes.put(id, v, function (err) {
                if (err) return monitor.emit('error', err)
                monitor.emit('finish')
              })
            })
          })
        })
      })
    }

    volumes.get(id, function (err, v) {
      if (err) return monitor.emit(new Error('Volume does not exist'))
      if (v.snapshot) return onindex(v)

      var space = crypto.randomBytes(32).toString('hex')
      var snapshotHash = crypto.createHash('sha256')
      var i = 0

      pump(
        metadata.createReadStream({
          gt: id + '!',
          lt: id + '!\xff'
        }),
        through.obj(function (file, enc, cb) {
          var name = file.key.slice(file.key.lastIndexOf('!') + 1)

          monitor.emit('index', name)

          getInode(id, file.value.ino || 0, function (err, data) {
            if (err && !err.notFound) return cb(err)

            var ondone = function () {
              var val = JSON.stringify({
                name: name,
                deleted: file.value.deleted,
                special: file.value.special,
                data: file.hash,
                mode: file.value.mode,
                rdev: file.value.rdev,
                uid: file.value.uid,
                gid: file.value.gid,
                ino: file.value.ino
              })

              snapshotHash.update(val)
              snapshots.put(space + '!' + lexint.pack(i++, 'hex'), val, cb)
            }

            if (!data || !data.data || file.value.special) return ondone()

            var hash = crypto.createHash('sha256')
            var rs = fs.createReadStream(path.join(home, data.data))

            rs.on('data', function (data) {
              hash.update(data)
            })
            rs.on('error', cb)
            rs.on('end', function () {
              file.hash = hash.digest('hex')
              ondone()
            })
          })
        }),
        function (err) {
          if (err) return monitor.emit('error', err)
          var key = snapshotHash.digest('hex')
          snapshots.put(key, space, function (err) {
            if (err) return monitor.emit('error', err)
            v.snapshot = key
            onindex(v)
          })
        }
      )
    })

    return monitor
  }

  cauf.nodes = function () {
    var write = function (node, enc, cb) {
      node.value = JSON.parse(node.value)
      cb(null, node)
    }

    return pump(log.createReadStream(), through.obj(write))
  }

  cauf.ancestors = function (key, cb) {
    var list = []

    var loop = function (key) {
      log.get(key, function (err, node) {
        if (err) return cb(err)
        list.unshift({node: node.key, snapshot: JSON.parse(node.value).snapshot})
        if (!node.links.length) return cb(null, list)
        loop(node.links[0])
      })
    }

    loop(key)
  }

  cauf.list = function () {
    return volumes.createKeyStream()
  }

  cauf.info = function (key, cb) {
    return volumes.get(key, cb)
  }

  cauf.remove = function (key, cb) {
    if (!cb) cb = noop

    var write = function (data, enc, cb) {
      metadata.del(data.key, cb)
    }

    pump(metadata.createReadStream({gt: key + '!', lt: key + '!\xff'}), through.obj(write), function (err) {
      if (err) return cb(err)
      volumes.del(key, cb)
    })
  }

  cauf.create = function (key, opts, cb) {
    if (typeof opts === 'function') return cauf.create(key, null, opts)
    if (!cb) cb = noop
    if (!opts) opts = {}
    volumes.get(key, function (_, v) {
      if (v) return cb(new Error('volume already exists'))
      volumes.put(key, {id: key, node: opts.node}, cb)
    })
  }

  cauf.replicate = function (opts) {
    if (!opts) opts = {}

    var plex = multiplex(function (stream, id) {
      var parts = id.split('/')

      if (parts[0] === 's') {
        var encode = function (data, enc, cb) {
          cb(null, JSON.stringify(data))
        }

        plex.emit('send-snapshot', parts[1])
        cauf.readSnapshot(parts[1], function (err, rs) {
          if (err) return stream.destroy(err)
          pump(rs, through.obj(encode), stream)
        })
        return
      }

      if (parts[0] === 'd') {
        plex.emit('send-data', parts[1])
        pump(cauf.createBlobReadStream(parts[1]), stream)
        return
      }
    })

    var logOutgoing = plex.createStream('hyperlog')
    var logIncoming = plex.receiveStream('hyperlog')

    var onnode = function (node, cb) {
      var value = JSON.parse(node.value.toString())
      var s = plex.createStream('s/' + value.snapshot)
      var hash = crypto.createHash('sha256')
      var space = crypto.randomBytes(32).toString('hex')
      var i = 0

      plex.emit('receive-snapshot', value.snapshot)

      var write = function (val, enc, cb) {
        var raw = val.toString()
        val = JSON.parse(raw)

        var snapshot = function () {
          hash.update(raw)
          snapshots.put(space + '!' + lexint.pack(i++, 'hex'), val, cb)
        }

        var done = function () {
          var meta = {special: val.special, deleted: val.deleted, mode: val.mode, uid: val.uid, gid: val.gid, ino: val.ino, rdev: val.rdev}
          cauf.put(value.snapshot, val.name, meta, function (err) {
            getInode(value.snapshot, val.ino, function (_, inode) {
              inode = inode || {refs: [], data: val.data && readablePath(val.data)}
              if (inode.refs.indexOf(val.name) === -1) inode.refs.push(val.name)
              putInode(value.snapshot, val.ino, inode, function (err) {
                if (err) return cb(err)
                snapshot()
              })
            })
          })
        }

        if (!val.data) return done()

        cauf.hasBlob(val.data, function (err, exists) {
          if (err) return cb(err)
          if (exists) return done()
          plex.emit('receive-data', val.data)
          pump(plex.createStream('d/' + val.data, {chunked: true}), cauf.createBlobWriteStream(function (err, key) {
            if (err) return cb(err)
            done()
          }))
        })
      }

      pump(s, through(write), function (err) {
        if (err) return cb(err)
        if (hash.digest('hex') !== value.snapshot) return cb(new Error('checksum mismatch'))
        plex.emit('node', node)
        snapshots.put(value.snapshot, space, function (err) {
          if (err) return cb(err)
          cb(null, node)
        })
      })
    }

    pump(logIncoming, log.replicate({live: opts.live, process: parallel(64, onnode)}), logOutgoing, function () {
      plex.end()
    })

    return plex
  }

  cauf.mount = function (key, mnt, opts) {
    if (!opts) opts = {}

    var mount = new events.EventEmitter()

    mount.id = null
    mount.layers = null
    mount.node = null
    mount.mountpoint = mnt
    mount.inodes = 0
    mount.unmount = cauf.unmount.bind(cauf, mnt)

    var wrap = function (cb) {
      return function (err) {
        if (err) return cb(fuse.errno(err.code))
        cb(0)
      }
    }

    var get = function (name, cb) {
      var loop = function (i) {
        if (i < 0) return cb(ENOENT)
        cauf.get(mount.layers[i], name, function (err, file) {
          if (err) return loop(i - 1)
          if (file.deleted) return cb(ENOENT)
          cb(null, file, mount.layers[i])
        })
      }

      loop(mount.layers.length - 1)
    }

    var del = function (name, ino, cb) {
      if (opts.debug) console.log('delete:', name)
      var oninode = function (err) {
        if (err) return cb(err)
        getInode(mount.id, ino, function (err, data) {
          if (err) return cb()
          var i = data.refs.indexOf(name)
          if (i < 0) throw new Error('BAD INODE: ' + name)
          data.refs.splice(i, 1)
          if (data.refs.length) return putInode(mount.id, ino, data, cb)
          delInode(mount.id, ino, function (err) {
            if (err) return cb(err)
            if (!data.data) return cb()
            fs.unlink(path.join(home, data.data), cb)
          })
        })
      }

      var loop = function (i) {
        if (i === mount.layers.length - 1) return cauf.del(mount.id, name, oninode)
        cauf.get(mount.layers[i], name, function (err, file) {
          if (err) return loop(i + 1)
          cauf.put(mount.id, name, {deleted: true}, oninode)
        })
      }

      loop(0)
    }

    var cow = function (name, cb) { // TODO: batch for me for speed/consistency
      get(name, function (err, file, layer) {
        if (err && name === '/') return cb(null, {mode: root.mode})
        if (err) return cb(err)
        if (layer === mount.id) return cb(null, file)

        if (opts.debug) console.log('copy-on-write:', name)

        var store = function (data) {
          if (data.refs.length === 1) {
            cauf.put(mount.id, name, file, function (err) {
              if (err) return cb(err)
              cb(null, file)
            })
            return
          }

          var i = 0
          var loop = function (err) {
            if (err) return cb(err)
            if (i === data.refs.length) return cb(null, file)
            var r = data.refs[i++]
            get(r, function (err, file) {
              if (err) return cb(err)
              cauf.put(mount.id, r, file, loop)
            })
          }

          loop(0)
        }

        var copy = function (from, to, cb) {
          mkdirp(path.join(home, to, '..'), function (err) {
            if (err) return cb(err)
            if (file.special) return mknod(path.join(home, to), file.mode, file.rdev, cb)
            pump(fs.createReadStream(path.join(home, from)), fs.createWriteStream(path.join(home, to)), cb)
          })
        }

        getInode(mount.id, file.ino, function (err) {
          if (!err) return cb(null, file) // already copied
          getInode(layer, file.ino, function (err, data) {
            if (err) return cb(err)

            if (!data.data) {
              putInode(mount.id, file.ino, data, function (err) {
                if (err) return cb(err)
                store(data)
              })
              return
            }

            var newPath = writeablePath()
            copy(data.data, newPath, function (err) {
              if (err) return cb(err)
              putInode(mount.id, file.ino, {refs: data.refs, data: newPath}, function (err) {
                if (err) return cb(err)
                store(data)
              })
            })
          })
        })
      })
    }

    var ready = function (root) {
      var ops = {}

      ops.force = true
      ops.options = ['suid', 'dev']

      ops.statfs = function (pathname, cb) { // TODO: return actual corrent data here instead
        cb(0, {
          bsize: 1000000,
          frsize: 1000000,
          blocks: 1000000,
          bfree: 1000000,
          bavail: 1000000,
          files: 1000000,
          ffree: 1000000,
          favail: 1000000,
          fsid: 1000000,
          flag: 1000000,
          namemax: 1000000
        })
      }

      ops.link = function (name, dest, cb) {
        if (opts.debug) console.log('link:', name, dest)

        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          cauf.put(mount.id, dest, file, function (err) {
            if (err) return cb(fuse.errno(err.code))
            getInode(mount.id, file.ino, function (err, data) {
              if (err) return cb(fuse.errno(err.code))
              data.refs.push(dest)
              putInode(mount.id, file.ino, data, wrap(cb))
            })
          })
        })
      }

      ops.fgetattr = function (name, fd, cb) {
        if (name === '/') return cb(0, root)

        get(name, function (err, file, layer) {
          if (err) return cb(fuse.errno(err.code))

          var nlink = 1
          var onstat = function (err, stat) {
            if (err) return cb(fuse.errno(err.code))
            cb(0, {
              mode: file.mode,
              size: file.size || stat.size,
              blksize: 4096,
              blocks: stat.blocks,
              dev: stat.dev,
              rdev: file.rdev || stat.rdev,
              nlink: nlink,
              ino: file.ino || stat.ino,
              uid: file.uid || process.getuid(),
              gid: file.gid || process.getgid(),
              mtime: new Date(file.mtime || 0),
              ctime: new Date(file.ctime || 0),
              atime: new Date(file.mtime || 0)
            })
          }

          if (file.mode & 040000) return onstat(null, root)
          getInode(layer, file.ino, function (err, inode) {
            if (err && fd > -1) return fs.fstat(fd, onstat)
            if (err) throw new Error('NO INODE FOR ' + name)
            if (err) return cb(fuse.errno(err.code))

            nlink = inode.refs.length
            if (fd < 0) fs.lstat(path.join(home, inode.data), onstat)
            else fs.fstat(fd, onstat)
          })
        })
      }

      ops.getattr = function (name, cb) {
        ops.fgetattr(name, -1, cb)
      }

      ops.readdir = function (name, cb) {
        if (!/\/$/.test(name)) name += '/'

        var key = toIndexKey(name)
        var result = []

        var stream = dirStream(mount.layers[mount.layers.length - 1], key)
        for (var i = mount.layers.length - 2; i >= 0; i--) {
          stream = union(stream, dirStream(mount.layers[i], key), toCompareKey)
        }

        stream.on('error', wrap(cb))

        stream.on('data', function (data) {
          if (data.value.deleted) return
          result.push(data.key.slice(data.key.lastIndexOf('/') + 1)) // haxx
        })

        stream.on('end', function () {
          cb(null, result)
        })
      }

      ops.truncate = function (name, size, cb) {
        if (opts.debug) console.log('truncate:', name, size)
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          getInode(mount.id, file.ino, function (err, data) {
            if (err) return cb(fuse.errno(err.code))
            fs.truncate(path.join(home, data.data), size, wrap(cb))
          })
        })
      }

      ops.ftruncate = function (name, fd, size, cb) {
        if (opts.debug) console.log('ftruncate:', name, fd, size)
        fs.ftruncate(fd, size, wrap(cb))
      }

      ops.fsync = function (name, fd, datasync, cb) {
        fs.fsync(fd, wrap(cb))
      }

      ops.rename = function (name, dest, cb) {
        if (opts.debug) console.log('rename:', name, dest)
        ops.link(name, dest, function (errno) {
          if (errno) return cb(errno)
          ops.unlink(name, cb)
        })
      }

      ops.mknod = function (name, mode, dev, cb) {
        if (opts.debug) console.log('mknod:', name, mode, dev)
        var inode = ++mount.inodes
        var filename = writeablePath()

        putInode(mount.id, inode, {data: filename, refs: [name]}, function (err) {
          if (err) return cb(fuse.errno(err.code))
          mkdirp(path.join(home, filename, '..'), function (err) {
            if (err) return cb(fuse.errno(err.code))
            mknod(path.join(home, filename), mode, dev, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cauf.put(mount.id, name, {special: true, rdev: dev, mode: mode, ino: inode}, wrap(cb))
            })
          })
        })
      }

      ops.open = function (name, flags, cb) {
        var open = function (layer, ino) {
          getInode(layer, ino, function (err, data) {
            if (err) return cb(fuse.errno(err.code))
            fs.open(path.join(home, data.data), flags, function (err, fd) {
              if (err) return cb(fuse.errno(err.code))
              cb(0, fd)
            })
          })
        }

        var readonly = function () {
          get(name, function (err, file, layer) {
            if (err) return cb(fuse.errno(err.code))
            if (file.special) return writeMaybe() // special file - always cow
            open(layer, file.ino)
          })
        }

        var writeMaybe = function () {
          cow(name, function (err, file) {
            if (err) return cb(fuse.errno(err))
            open(mount.id, file.ino)
          })
        }

        if (flags === 0) readonly() // readonly
        else writeMaybe() // cow
      }

      ops.create = function (name, mode, cb) {
        if (opts.debug) console.log('create:', name, mode)
        var inode = ++mount.inodes
        var filename = writeablePath()

        putInode(mount.id, inode, {data: filename, refs: [name]}, function (err) {
          if (err) return cb(fuse.errno(err.code))
          mkdirp(path.join(home, filename, '..'), function (err) {
            if (err) return cb(fuse.errno(err.code))
            fs.open(path.join(home, filename), 'w+', mode, function (err, fd) {
              if (err) return cb(fuse.errno(err.code))
              cauf.put(mount.id, name, {mode: mode, ino: inode}, function (err) {
                if (err) return cb(fuse.errno(err.code))
                cb(0, fd)
              })
            })
          })
        })
      }

      ops.unlink = function (name, cb) {
        if (opts.debug) console.log('unlink:', name)
        cow(name, function (err, file) { // TODO: don't copy file if refs === 1 and deleting
          if (err) return cb(fuse.errno(err.code))
          del(name, file.ino, wrap(cb))
        })
      }

      ops.mkdir = function (name, mode, cb) {
        if (opts.debug) console.log('mkdir:', name, mode)
        var inode = ++mount.inodes
        putInode(mount.id, inode, {refs: [name]}, function (err) {
          if (err) return cb(fuse.errno(err.code))
          cauf.put(mount.id, name, {mode: mode | 040000, ino: inode}, wrap(cb))
        })
      }

      ops.rmdir = function (name, cb) {
        if (opts.debug) console.log('rmdir:', name)
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          del(name, file.ino, wrap(cb))
        })
      }

      ops.write = function (name, fd, buf, len, offset, cb) {
        fs.write(fd, buf, 0, len, offset, function (err, bytes) {
          if (err) return cb(fuse.errno(err.code))
          cb(bytes)
        })
      }

      ops.read = function (name, fd, buf, len, offset, cb) {
        fs.read(fd, buf, 0, len, offset, function (err, bytes) {
          if (err) return cb(fuse.errno(err.code))
          cb(bytes)
        })
      }

      ops.release = function (name, fd, cb) {
        fs.close(fd, wrap(cb))
      }

      ops.symlink = function (name, dest, cb) {
        if (opts.debug) console.log('symlink:', name, dest)
        ops.create(dest, 41453, function (errno, fd) {
          if (errno) return cb(errno)

          var buf = new Buffer(name)
          var pos = 0
          var loop = function () {
            fs.write(fd, buf, 0, buf.length, pos, function (err, bytes) {
              if (err) return cb(fuse.errno(err.code))
              if (bytes === buf.length) return fs.close(fd, wrap(cb))
              pos += bytes
              buf = buf.slice(bytes)
              loop()
            })
          }

          loop()
        })
      }

      ops.readlink = function (name, cb) {
        get(name, function (err, file, layer) {
          if (err) return cb(fuse.errno(err.code))
          getInode(layer, file.ino, function (err, data) {
            if (err) return cb(fuse.errno(err.code))
            fs.readFile(path.join(home, data.data), 'utf-8', function (err, res) {
              if (err) return cb(fuse.errno(err.code))
              cb(0, res)
            })
          })
        })
      }

      ops.chmod = function (name, mode, cb) {
        if (opts.debug) console.log('chmod:', name, mode)
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          file.mode = mode
          cauf.put(mount.id, name, file, wrap(cb))
        })
      }

      ops.chown = function (name, uid, gid, cb) {
        if (opts.debug) console.log('chown:', name, mode)
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          if (uid > -1) file.uid = uid
          if (gid > -1) file.gid = gid
          cauf.put(mount.id, name, file, wrap(cb))
        })
      }

      ops.utimens = function (name, ctime, mtime, cb) {
        if (opts.time === false) return cb(0)
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          file.ctime = ctime.getTime()
          file.mtime = mtime.getTime()
          cauf.put(mount.id, name, file, wrap(cb))
        })
      }

      fuse.mount(mnt, ops, function (err) {
        if (err) return mount.emit('error', err)
        mount.emit('ready')
      })
    }

    var onlayers = function (err, layers) {
      if (err) return mount.emit('error', err)

      var toSnapshot = function (val) {
        return val.snapshot
      }

      var toNode = function (val) {
        return val.node
      }

      mount.layers = layers.map(toSnapshot).concat(mount.id) // push writable layer
      mount.nodes = layers.map(toNode)

      var done = function () {
        mkdirp(mnt, function (err) {
          if (err) return mount.emit('error', err)
          fs.stat(mnt, function (err, st) {
            if (err) return mount.emit('error', err)
            ready(st)
          })
        })
      }

      mount.inodes = 1024
      var loop = function (i) {
        if (i < 0) return done()
        countInodes(mount.layers[i], function (_, cnt) {
          if (cnt) mount.inodes = Math.max(cnt, mount.inodes)
          loop(i - 1)
        })
      }

      loop(mount.layers.length - 1)
    }

    volumes.get(key, function (err, v) {
      if (err) return mount.emit('error', new Error('Volume does not exist'))

      mount.id = key
      mount.mountpoint = mnt
      mount.node = v.node
      if (!v.node) return onlayers(null, [])
      cauf.ancestors(v.node, onlayers)
    })

    return mount
  }

  return cauf
}

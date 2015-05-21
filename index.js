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
    return fs.createReadStream(path.join(home, readablePath(hash)))
  }

  cauf.createBlobWriteStream = function () {
    if (!cb) cb = noop
    var filename = path.join(os.tmpdir(), 'cauf-tmp-' + crypto.randomBytes(32).digest('hex'))
    var hash = crypto.createHash('sha256')

    var write = function (data, enc, cb) {
      hash.update(data)
      cb(null, data)
    }

    var stream = pumpify(through(write), fs.createWriteStream(filename))

    stream.on('prefinish', function () {
      stream.cork()
      stream.key = hash.digest('hex')
      fs.rename(filename, path.join(home, readablePath(stream.key)), function (err) {
        if (err) return stream.destroy(err)
        stream.uncork()
      })
    })

    return ws
  }

  cauf.createSnapshopWriteStream = function () {

  }

  cauf.createSnapshopReadStream = function (key) {
    return from.obj(function (size, cb) {
      if (!key) return cb(null, null)
      snapshots.get(key, function (err, data) {
        if (err) return cb(err)
        data = JSON.parse(data)
        key = data.prev
        cb(null, data)
      })
    })
  }

  cauf.snapshot = function (id) { // don't mutate the layer while running this for now
    var monitor = new events.EventEmitter()

    var onindex = function (v) {
      var key = monitor.key = v.snapshot

      if (!v.snapshot) return monitor.emit('finish')

      volumes.put(id, v, function (err) {
        if (err) return monitor.emit('error', err)

        pump(
          cauf.createSnapshopReadStream(key),
          through.obj(function (data, enc, cb) {
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
          }),
          function () {
            var done = function (err) {
              if (err) return monitor.emit('error', err)
              volumes.put(id, v, function (err) {
                if (err) return monitor.emit('error', err)
                monitor.emit('finish')
              })
            }

            var oldSnapshot = v.ancestor
            v.ancestor = key
            v.snapshot = null

            if (oldSnapshot) ancestors.put(key, oldSnapshot, done)
            else ancestors.put(key, key, done)
          }
        )
      })
    }

    volumes.get(id, function (err, v) {
      if (err) return monitor.emit(new Error('Volume does not exist'))
      if (v.snapshot) return onindex(v)

      var key = null

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
                prev: key,
                mode: file.value.mode,
                rdev: file.value.rdev,
                uid: file.value.uid,
                gid: file.value.gid,
                ino: file.value.ino
              })

              key = crypto.createHash('sha256').update(val).digest('hex')
              snapshots.put(key, val, cb)
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
          v.snapshot = key
          onindex(v)
        }
      )
    })

    return monitor
  }

  cauf.listSnapshots = function () {
    return ancestors.createKeyStream()
  }

  cauf.ancestors = function (key, cb) {
    var list = []

    var loop = function (key) {
      snapshots.get(key, function (_, snapshot) {
        if (snapshot) list.unshift(key) // is a snapshot \o/
        ancestors.get(key, function (_, parent) {
          if (parent && parent !== key) return loop(parent)
          cb(null, list)
        })
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
      volumes.put(key, {id: key, ancestor: opts.ancestor}, cb)
    })
  }

  cauf.mount = function (key, mnt, opts) {
    if (!opts) opts = {}

    var mount = new events.EventEmitter()

    mount.id = null
    mount.layers = null
    mount.ancestor = null
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
      console.log('DELETING', name, ino)
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

      ops.link = function (name, dest, cb) {
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
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          getInode(mount.id, file.ino, function (err, data) {
            if (err) return cb(fuse.errno(err.code))
            fs.truncate(path.join(home, data.data), size, wrap(cb))
          })
        })
      }

      ops.ftruncate = function (name, fd, size, cb) {
        fs.ftruncate(fd, size, wrap(cb))
      }

      ops.fsync = function (name, fd, datasync, cb) {
        fs.fsync(fd, wrap(cb))
      }

      ops.rename = function (name, dest, cb) {
        ops.link(name, dest, function (errno) {
          if (errno) return cb(errno)
          ops.unlink(name, cb)
        })
      }

      ops.mknod = function (name, mode, dev, cb) {
        console.log('mknod', name, mode, dev)
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
        cow(name, function (err, file) { // TODO: don't copy file if refs === 1 and deleting
          if (err) return cb(fuse.errno(err.code))
          del(name, file.ino, wrap(cb))
        })
      }

      ops.mkdir = function (name, mode, cb) {
        var inode = ++mount.inodes
        putInode(mount.id, inode, {refs: [name]}, function (err) {
          if (err) return cb(fuse.errno(err.code))
          cauf.put(mount.id, name, {mode: mode | 040000, ino: inode}, wrap(cb))
        })
      }

      ops.rmdir = function (name, cb) {
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
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          file.mode = mode
          cauf.put(mount.id, name, file, wrap(cb))
        })
      }

      ops.chown = function (name, uid, gid, cb) {
        cow(name, function (err, file) {
          if (err) return cb(fuse.errno(err.code))
          file.uid = uid
          file.gid = gid
          cauf.put(mount.id, name, file, wrap(cb))
        })
      }

      ops.utimens = function (name, ctime, mtime, cb) {
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

      mount.layers = layers.concat(mount.id) // push writable layer

      var done = function (ino) {
        mount.inodes = ino
        mkdirp(mnt, function (err) {
          if (err) return mount.emit('error', err)
          fs.stat(mnt, function (err, st) {
            if (err) return mount.emit('error', err)
            ready(st)
          })
        })
      }

      var loop = function (i) {
        if (i < 0) return done(1024)
        countInodes(mount.layers[mount.layers.length - i], function (_, cnt) {
          if (cnt) return done(cnt)
          loop(i - 1)
        })
      }

      loop(mount.layers.length - 1)
    }

    volumes.get(key, function (err, v) {
      if (err) return mount.emit('error', new Error('Volume does not exist'))
      mount.id = key
      mount.mountpoint = mnt
      if (!v.ancestor) return onlayers(null, [])
      cauf.ancestors(v.ancestor, onlayers)
    })

    return mount
  }

  return cauf
}

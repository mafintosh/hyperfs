var path = require('path')
var level = require('level')
var crypto = require('crypto')
var mkdirp = require('mkdirp')
var pumpify = require('pumpify')
var pump = require('pump')
var cuid = require('cuid')
var fs = require('fs')
var fuse = require('fuse-bindings')
var lexint = require('lexicographic-integer')
var union = require('sorted-union-stream')
var events = require('events')
var mknod = require('mknod')
var subleveldown = require('subleveldown')
var enumerate = require('level-enumerate')

var noop = function () {}
var ENOENT = new Error('ENOENT')
ENOENT.code = 'ENOENT'

module.exports = function (home) {
  var that = {}

  var db = level(path.join(home, 'db'))
  var blobs = path.join(home, 'blobs')
  var mounts = path.join(home, 'mounts')

  var getId = enumerate(subleveldown(db, 'ids'))
  var files = subleveldown(db, 'files', {valueEncoding: 'json'})
  var parents = subleveldown(db, 'parents')

  var blobPath = function (hash) {
    return path.join(blobs, hash.slice(0, 2), hash.slice(2, 4), hash.slice(4))
  }

  var toIndexKey = function(name) {
    var depth = name.split('/').length - 1
    return lexint.pack(depth, 'hex') + '!' + name
  }

  that.add = function (layer, filename, stat, cb) {
    var key = layer + '!' + toIndexKey(filename)
    stat.ctime = stat.ctime || Date.now()
    stat.mtime = stat.mtime || Date.now()
    console.log('add', key)
    files.put(key, stat, cb)
  }

  that.get = function (layer, filename, cb) {
    var key = layer + '!' + toIndexKey(filename)
    console.log('get', key)
    files.get(key, cb)
  }

  that.del = function (layer, filename, cb) {
    var key = layer + '!' + toIndexKey(filename)
    files.del(key, cb)
  }

  that.createLayerStream = function (layer) {

  }

  that.createBlobReadStream = function (hash) {
    return fs.createReadStream(blobPath(hash))
  }

  that.createBlobWriteStream = function () {
    var sha = crypto.createHash('sha256')
    var tmp = path.join(os.tmpdir(), 'cauf-file-' + cuid())
    var ws = fs.createWriteStream(tmp)

    ws.on('finish', function () {
      stream.cork()
      stream.key = sha.digest('hex')
      var filename = blobPath(stream.key)
      mkdir(path.join(filename, '..'), function (err) {
        if (err) return stream.destroy(err)
        fs.rename(tmp, filename, function (err) {
          if (err) return stream.destroy(err)
          stream.uncork()
        })
      })
    })

    var stream = pumpify(through(write), ws)
    return stream
  }

  that.unmount = function (mnt, cb) {
    fuse.unmount(mnt, cb)
  }

  that.mount = function (mnt, opts) {
    if (!opts) opts = {}

    var mount = new events.EventEmitter()

    mount.id = null
    mount.writes = null
    mount.layers = null

    mount.unmount = function (cb) {
      that.unmount(mnt, cb)
    }

    var cow = function (name, cb) {
      that.get(mount.id, name, function (err, file) {
        if (file) return cb(null, file)

        console.log('copy-on-write for', mount.id, name)

        var copy = function (file) {
          var done = function (err) {
            if (err) return cb(err)

            var newFile = {
              mode: file ? file.mode : 33188,
              rdev: file && file.rdev,
              uid: file && file.uid,
              gid: file && file.gid,
              blob: newBlob
            }

            that.add(mount.id, name, newFile, function (err) {
              if (err) return cb(err)
              cb(null, newFile)
            })
          }

          mkdirp(path.join(newBlob, '..'), function (err) {
            if (err) return cb(err)
            if (!file) return done()
            if (file.rdev) return mknod(newBlob, file.mode, file.rdev, done) // special file
            pump(fs.createReadStream(file.blob), fs.createWriteStream(newBlob), done)
          })
        }

        var newBlob = path.join(mount.writes, name)
        var loop = function (i) {
          if (i < 0) return copy(null)
          that.get(mount.layers[i], name, function (err, file) {
            if (err) return loop(i - 1)
            copy(file.deleted ? null : file)
          })
        }

        loop(mount.layers.length - 2)
      })
    }

    var toCompareKey = function (data) {
      return data.key.slice(data.key.indexOf('!') + 1)
    }

    var createDirStream = function (layer, key) {
      return files.createReadStream({
        gt: layer + '!' + key,
        lt: layer + '!' + key + '\xff'
      })
    }

    var get = function (name, cb) {
      var loop = function (i) {
        if (i < 0) return cb(ENOENT)
        that.get(mount.layers[i], name, function (err, file) {
          if (err) return loop(i - 1)
          if (file.deleted) return cb(ENOENT)
          cb(null, file)
        })
      }

      loop(mount.layers.length - 1)
    }

    var del = function (name, cb) {
      var loop = function (i) {
        if (i < 0) return that.del(mount.id, name, cb)
        that.get(mount.layers[i], name, function (err, file) {
          if (err) return loop(i - 1)
          that.put(mount.id, name, {deleted: true}, cb)
        })
      }

      loop(mount.layers.length - 2)
    }

    var ready = function (root) {
      fuse.mount(mnt, {
        force: true,
        options: ['suid', 'dev'],
        getattr: function (name, cb) {
          if (name === '/') return cb(0, root)

          get(name, function (err, file) {
            if (err) return cb(fuse.errno(err.code))

            var onstat = function (_, st) {
              if (!st) st = root
              cb(0, {
                mode: file.mode,
                size: st.size,
                uid: file.uid || st.uid,
                gid: file.gid || st.gid,
                dev: file.dev || st.dev,
                nlink: file.nlink || st.nlink,
                rdev: file.rdev || st.rdev,
                blksize: file.blksize || st.blksize,
                blocks: file.blocks || st.blocks,
                ino: file.ino || st.ino,
                ctime: new Date(file.ctime || st.ctime.getTime()),
                mtime: new Date(file.mtime || st.mtime.getTime()),
                atime: file.atime || new Date()
              })
            }

            if (file.blob) fs.lstat(file.blob, onstat)
            else onstat(null, root)
          })
        },
        symlink: function (name, dst, cb) {
          cow(dst, function (err, file) { // cow is not needed - just use to get a file handle as it'll never cpy
            if (err) return cb(fuse.errno(err.code))
            fs.writeFile(file.blob, name, function (err) {
              if (err) return cb(fuse.errno(err.code))
              that.add(mount.id, dst, {mode: 41453, blob: file.blob}, function (err) {
                if (err) return cb(fuse.errno(err.code))
                cb(0)
              })
            })
          })
        },
        readlink: function (name, cb) {
          get(name, function (err, file) {
            if (err) return cb(fuse.errno(err.code))
            fs.readFile(file.blob, 'utf-8', function (err, link) {
              if (err) return cb(fuse.errno(err.code))
              cb(0, link)
            })
          })
        },
        link: function (name, dst, cb) {
          // doesn't work
          // get(name, function (err, file) {
          //   if (err) return cb(fuse.errno(err.code))
          //   fs.link(file.blob, path.join(mount.writes, dst), function (err) {
          //     if (err) return cb(fuse.errno(err.code))
          //     that.add(mount.id, dst, file, function (err) {
          //       if (err) return cb(fuse.errno(err.code))
          //       cb(0)
          //     })
          //   })
          // })
          console.error('hardlinks not implemented yet')
          cb(fuse.EPERM)
        },
        rename: function (from, to, cb) {
          cow(from, function (err, file) {
            if (err) return cb(fuse.errno(err.code))
            var newBlob = path.join(mount.writes, to)
            fs.rename(file.blob, newBlob, function (err) {
              if (err) return cb(fuse.errno(err.code))
              file.blob = newBlob
              that.del(mount.id, from, function (err) {
                if (err) return cb(fuse.errno(err.code))
                that.add(mount.id, to, file, function (err) {
                  if (err) return cb(fuse.errno(err.code))
                  cb(0)
                })
              })
            })
          })
        },
        readdir: function (name, cb) {
          if (!/\/$/.test(name)) name += '/'
          var key = toIndexKey(name)

          var result = []
          var stream = createDirStream(mount.layers[mount.layers.length - 1], key)
          for (var i = mount.layers.length - 2; i >= 0; i--) {
            stream = union(stream, createDirStream(mount.layers[i], key), toCompareKey)
          }

          stream.on('error', function (err) {
            cb(fuse.errno(err.code))
          })

          stream.on('data', function (data) {
            if (data.value.deleted) return
            result.push(data.key.slice(data.key.lastIndexOf('/') + 1)) // haxx
          })

          stream.on('end', function () {
            cb(null, result)
          })
        },
        mknod: function (name, mode, dev, cb) {
          console.log('calling mknod', name)
          var blob = path.join(mount.writes, name)
          mknod(blob, mode, dev, function (err) {
            if (err) return cb(fuse.errno(err.code))
            that.add(mount.id, name, {mode: mode, rdev: dev, blob: blob}, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cb(0)
            })
          })
        },
        open: function (name, flags, cb) {
          var readonly = function () {
            get(name, function (err, file) {
              if (err) return cb(fuse.errno(err.code))
              if (file.rdev) return writeMaybe() // special file - always cow

              fs.open(file.blob, 'r', function (err, fd) {
                if (err) return cb(fuse.errno(err.code))
                cb(0, fd)
              })
            })
          }

          var writeMaybe = function () {
            cow(name, function (err, file) {
              if (err) return cb(fuse.errno(err))
              fs.open(file.blob, flags, function (err, fd) {
                if (err) return cb(fuse.errno(err))
                cb(0, fd)
              })
            })
          }

          if (flags === 0) readonly() // readonly
          else writeMaybe() // cow
        },
        read: function (name, fd, buf, len, offset, cb) {
          fs.read(fd, buf, 0, len, offset, function (err, bytes) {
            if (err) return cb(fuse.errno(err.code))
            cb(bytes)
          })
        },
        release: function (name, fd, cb) {
          fs.close(fd, function (err) {
            if (err) return cb(fuse.errno(err.code))
            cb(0)
          })
        },
        create: function (name, mode, cb) {
          var blob = path.join(mount.writes, name)
          mkdirp(path.join(blob, '..'), function (err) {
            if (err) return cb(fuse.errno(err.code))
            fs.open(blob, 'w', mode, function (err, fd) {
              if (err) return cb(fuse.errno(err.code))
              that.add(mount.id, name, {mode: mode, blob: blob}, function (err) {
                if (err) return cb(fuse.errno(err.code))
                cb(0, fd)
              })
            })
          })
        },
        write: function (name, fd, buf, len, offset, cb) {
          fs.write(fd, buf, 0, len, offset, function (err, bytes) {
            if (err) return cb(fuse.errno(err.code))
            cb(bytes)
          })
        },
        unlink: function (name, cb) {
          var blob = path.join(mount.writes, name)
          fs.unlink(blob, function () { // ignore error as this is just gc
            del(name, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cb(0)
            })
          })
        },
        mkdir: function (name, mode, cb) {
          that.add(mount.id, name, {mode: mode | 040000}, function (err) {
            if (err) return cb(fuse.errno(err.code))
            cb(0)
          })
        },
        rmdir: function (name, cb) {
          var blob = path.join(mount.writes, name)
          fs.rmdir(blob, function (err) { // ignore error as this is just gc
            del(name, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cb(0)
            })
          })
        },
        utimens: function (name, ctime, mtime, cb) {
          cow(name, function (err, file) {
            if (err) return cb(fuse.errno(err.code))
            file.ctime = ctime.getTime()
            file.mtime = mtime.getTime()
            that.add(mount.id, name, file, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cb(0)
            })
          })
        },
        chmod: function (name, mode, cb) {
          cow(name, function (err, file) {
            if (err) return cb(fuse.errno(err.code))
            file.mode = mode
            that.add(mount.id, name, file, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cb(0)
            })
          })
        },
        chown: function (name, uid, gid, cb) {
          cow(name, function (err, file) {
            if (err) return cb(fuse.errno(err.code))
            file.uid = uid
            file.gid = gid
            that.add(mount.id, name, file, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cb(0)
            })
          })
        },
        truncate: function (name, size, cb) {
          cow(name, function (err, file) {
            if (err) return cb(fuse.errno(err))
            fs.truncate(file.blob, size, function (err) {
              if (err) return cb(fuse.errno(err.code))
              cb(0)
            })
          })
        }
      }, function (err) {
        if (err) mount.emit('error', err)
        else mount.emit('ready')
      })
    }

    var onid = function (err, id) {
      if (err) return mount.emit('error', err)

      mount.id = id.toString()
      mount.writes = path.join(mounts, mount.id)
      mount.layers = [].concat(opts.layers || [], mount.id)
      mount.mountpoint = mnt

      mkdirp(mnt, function (err) {
        if (err) return mount.emit('error', err)
        mkdirp(mount.writes, function (err) {
          if (err) return mount.emit('error', err)
          fs.stat(mnt, function (err, st) {
            if (err) return mount.emit('error', err)
            ready(st)
          })
        })
      })
    }

    if (opts.id) return onid(null, opts.id)
    getId(mnt, onid)

    return mount
  }

  return that
}

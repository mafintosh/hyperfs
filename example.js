require('mkdirp').sync('./cauf')
require('mkdirp').sync('./cauf2')

var cauf = require('./')('./cauf')
var cauf2 = require('./')('./cauf2')

var s = cauf.replicate()

s.pipe(cauf2.replicate()).pipe(s)

// // return
// cauf.create('test5', {ancestor: '0f535d6400e9fb1ba94fbe035f0d0c402e4cbcdf716d77488205945585deb8af'}, function () {
//   var mount = cauf.mount('test5', 'mnt')

//   mount.on('ready', function () {
//     // console.log('ready')
//     // cauf.snapshot(mount.id).on('finish', function () {
//     //   console.log('snapshot finished', this.key)
//     // })
//     console.log('Mounted ' + mount.id + ' on ./mnt')
//   })

//   process.on('SIGINT', function () {
//     cauf.unmount('mnt', function () {
//       process.exit()
//     })
//   })
// })

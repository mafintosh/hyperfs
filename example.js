require('mkdirp').sync('./cauf')

var cauf = require('./')('./cauf')

var mount = cauf.mount('mnt')

mount.on('ready', function () {
  console.log('Mounted ' + mount.id + ' on ./mnt')
})

process.on('SIGINT', function () {
  cauf.unmount('mnt', function () {
    process.exit()
  })
})
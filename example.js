require('mkdirp').sync('./cauf')

var cauf = require('./')('./cauf')

cauf.mount('mnt')

process.on('SIGINT', function () {
  cauf.unmount('mnt', function () {
    process.exit()
  })
})
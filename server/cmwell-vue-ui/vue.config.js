var filter = function(pathname, req) {
  return (
    !(
      Object.keys(req.query).length === 0 && req.query.constructor === Object
    ) ||
    pathname.startsWith('/proc') ||
    pathname.startsWith('/health') ||
    pathname.startsWith('/meta/app')
  )
}

module.exports = {
  pluginOptions: {
    proxy: {
      context: filter,
      options: {
        target: process.env.VUE_APP_CMWELL_URL,
        logLevel: 'debug'
      }
    }
  },
  publicPath: process.env.VUE_APP_DEPLOYED_URL
}

// module.exports = {
//   devServer: {
//     proxy: 'http://xch0:9000'
//   }
// }

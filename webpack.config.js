const slsw = require('serverless-webpack')
const nodeExternals = require('webpack-node-externals')

module.exports = (async () => {
  return {
    entry: slsw.lib.entries,
    target: 'node',
    devtool: 'hidden-source-map',
    mode: slsw.lib.webpack.isLocal ? 'development' : 'production',
    externals: [nodeExternals()],
    performance: {
      hints: false
    },
    optimization: {
      minimize: true,
      usedExports: true
    }
  }
})()

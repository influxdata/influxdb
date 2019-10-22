module.exports = (() => {
  const GIT_SHA =
    process.env.INFLUXDB_SHA ||
    require('child_process')
      .execSync('git rev-parse HEAD')
      .toString()

  // Webpack has some specific rules about formatting
  // lets protect our developers from that!
  function formatStatic(dir) {
    if (!dir.length) {
      return dir
    }

    // NOTE: slices for sideeffect free editing (alex)
    let _dir = dir.slice(0)

    if (_dir[0] === '/') {
      _dir = _dir.slice(1)
    }

    if (_dir[_dir.length - 1] === '/') {
      _dir = _dir.slice(0, -1)
    }

    return _dir + '/'
  }

  // Webpack has some specific rules about formatting
  // lets protect our developers from that!
  function formatBase(prefix) {
    if (prefix === '/') {
      return prefix
    }

    // NOTE: slices for sideeffect free editing (alex)
    let _prefix = prefix.slice(0)

    if (_prefix[0] !== '/') {
      _prefix = '/' + _prefix
    }

    if (_prefix[_prefix.length - 1] !== '/') {
      _prefix = _prefix + '/'
    }

    return _prefix
  }

  const STATIC_DIRECTORY = formatStatic(process.env.STATIC_DIRECTORY || '')
  const BASE_PATH = formatBase(process.env.BASE_PATH || '/')
  const API_BASE_PATH = formatBase(process.env.API_BASE_PATH || '/')

  return {
    formatStatic,
    formatBase,
    GIT_SHA,
    STATIC_DIRECTORY,
    BASE_PATH,
    API_BASE_PATH,
  }
})()

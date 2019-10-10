export {}

const GIT_SHA =
  process.env.INFLUXDB_SHA ||
  require('child_process')
    .execSync('git rev-parse HEAD')
    .toString()

const STATIC_DIRECTORY = (dir => {
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

  return _dir
})(process.env.STATIC_DIRECTORY || '')

const BASE_PATH = (prefix => {
  // TODO: adding a basePath feature in the @influxdata/oats
  // project is currently required before turning this on
  // just remove this return when that is done (alex)
  return '/'

  /* eslint-disable no-unreachable */
  if (prefix === '/') {
    return prefix
  }

  // NOTE: slices for sideeffect free editing (alex)
  let _prefix = prefix.slice(0)

  if (_prefix[0] !== '/') {
    _prefix = '/' + _prefix
  }

  if (_prefix[prefix.length - 1] !== '/') {
    _prefix = _prefix + '/'
  }

  return _prefix
  /* eslint-enable no-unreachable */
})(process.env.BASE_PATH || '/')

module.exports = {
  GIT_SHA,
  STATIC_DIRECTORY,
  BASE_PATH,
}

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

  return (
    dir.slice(
      dir[0] === '/' ? 1 : 0,
      dir[dir.length - 1] === '/' ? -1 : undefined
    ) + '/'
  )
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

  return prefix[0] === '/'
    ? prefix[prefix.length - 1] === '/'
      ? prefix
      : prefix + '/'
    : prefix[prefix.length - 1] === '/'
    ? '/' + prefix
    : '/' + prefix + '/'
  /* eslint-enable no-unreachable */
})(process.env.BASE_PATH || '/')

module.exports = {
  GIT_SHA,
  STATIC_DIRECTORY,
  BASE_PATH,
}

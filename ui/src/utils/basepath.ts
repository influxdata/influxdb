import {BASE_PATH} from 'src/shared/constants'

export const getBasepath = () => {
  if (BASE_PATH === '/') {
    return ''
  }

  return BASE_PATH.slice(0, -1)
}

export const stripPrefix = (pathname, basepath = getBasepath()) => {
  if (basepath === '') {
    return pathname
  }

  const expr = new RegExp(`^${basepath}`)
  const matches = pathname.match(expr)
  if (matches) {
    return pathname.replace(expr, '')
  }
}

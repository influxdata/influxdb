import {getRootNode} from 'src/utils/nodes'
import {BASE_PATH, API_BASE_PATH} from 'src/shared/constants'

export const getBrowserBasepath = () => {
  const rootNode = getRootNode()
  return rootNode.getAttribute('data-basepath') || ''
}

export const getBasepath = () => {
  if (!BASE_PATH || BASE_PATH === '/') {
    return ''
  }

  return BASE_PATH.slice(0, -1)
}

export const getAPIBasepath = () => {
  if (!API_BASE_PATH || API_BASE_PATH === '/') {
    return ''
  }

  return API_BASE_PATH.slice(0, -1)
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

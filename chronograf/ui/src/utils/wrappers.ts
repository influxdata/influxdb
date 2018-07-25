import _ from 'lodash'

export function getDeep<T = any>(
  obj: any,
  path: string | number,
  fallback: T
): T {
  return _.get<T>(obj, path, fallback)
}

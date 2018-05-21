import _ from 'lodash'

export function getNested<T = any>(obj: any, path: string, fallback: T): T {
  return _.get<T>(obj, path, fallback)
}

import _ from 'lodash'

export function getNested<T = any>(obj: any, path: string, fallack: T): T {
  return _.get<T>(obj, path, fallack)
}

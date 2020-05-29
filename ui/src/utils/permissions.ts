import {Permission} from '@influxdata/influx'
import {get} from 'lodash'

export const permissions = (
  permissions: Permission[]
): {[x: string]: Permission.ActionEnum[]} => {
  const p = permissions.reduce((acc, {action, resource}) => {
    const {type} = resource
    const name = get(resource, 'name', '')
    let key = `${type}`
    if (name) {
      key = `${type}-${name}`
    }

    let actions = get(acc, key, [])

    if (name && actions) {
      return {...acc, [key]: [...actions, action]}
    }

    actions = get(acc, key || resource.type, [])
    return {...acc, [type]: [...actions, action]}
  }, {})
  return p
}

import {orderBy, get, toLower} from 'lodash'

export enum SortTypes {
  String = 'string',
}

export const sortSelector = (_, props) => ({
  key: props.sortKey,
  direction: props.sortDirection,
  type: props.sortType,
})

function orderByType(data, type) {
  switch (type) {
    case 'string':
      return toLower(data)
    case 'date':
      return Date.parse(data)
    case 'float':
      return parseFloat(data)
    default:
      return data
  }
}

export function getSortedResources<T>(
  resourceList: T[],
  sortKey: string,
  sortDirection: string,
  sortType: string
): T[] {
  if (sortKey && sortDirection) {
    return orderBy<T>(
      resourceList,
      r => orderByType(get(r, sortKey), sortType),
      [sortDirection]
    ).map(r => r)
  }
  return resourceList
}

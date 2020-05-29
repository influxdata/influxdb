import {orderBy, get, toLower} from 'lodash'

export enum SortTypes {
  String = 'string',
  Date = 'date',
  Float = 'float',
}

function orderByType(data, type) {
  switch (type) {
    case SortTypes.String:
      return toLower(data)
    case SortTypes.Date:
      return Date.parse(data)
    case SortTypes.Float:
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
    )
  }
  return resourceList
}

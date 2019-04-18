import {createSelector} from 'reselect'
import {orderBy, get, toLower} from 'lodash'

const resourceList = state => state

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

export const getSortedResources = createSelector(
  resourceList,
  sortSelector,
  (resourceList, sort) => {
    if (sort.key && sort.direction) {
      return orderBy<{id: string}>(
        resourceList,
        r => orderByType(get(r, sort.key), sort.type),
        [sort.direction]
      ).map(r => r)
    }

    return resourceList
  }
)

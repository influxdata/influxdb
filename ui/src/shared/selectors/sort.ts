import {createSelector} from 'reselect'
import {orderBy, get} from 'lodash'

const resourceList = state => state

export const sortSelector = (_, props) => ({
  key: props.sortKey,
  direction: props.sortDirection,
  type: props.sortType,
})

function orderByType(data, type) {
  switch (type) {
    case 'date':
      return Date.parse(data)
    case 'float':
      return parseFloat(data)
    default:
      return data
  }
}

export const getSortedResource = createSelector(
  resourceList,
  sortSelector,
  (resourceList, sort) => {
    if (sort.key && sort.direction) {
      return orderBy<{id: string}>(
        resourceList,
        r => orderByType(get(r, sort.key), sort.type),
        [sort.direction]
      ).map(r => r.id)
    }

    return resourceList
  }
)

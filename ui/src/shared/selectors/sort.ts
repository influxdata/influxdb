import {createSelector} from 'reselect'
import orderBy from 'lodash/orderBy'

const resourceList = state => state

export const sortSelector = (_, props) => ({
  key: props.sortKey,
  direction: props.sortDirection,
})

export const getSortedResource = createSelector(
  resourceList,
  sortSelector,
  (resourceList, sort) => {
    if (sort.key && sort.direction) {
      return orderBy(resourceList, [sort.key], [sort.direction])
    }

    return resourceList
  }
)

import {PureComponent} from 'react'
import _ from 'lodash'

// searchKeys: the keys whose values you want to filter on
// if the values are nested use dot notation i.e. tasks.org.name

interface Props<T> {
  list: T[]
  searchTerm: string
  searchKeys: string[]
  children: (list: T[]) => any
}

export default class FilterList<T> extends PureComponent<Props<T>> {
  public render() {
    return this.props.children(this.filtered)
  }

  private get filtered(): T[] {
    const {list, searchKeys, searchTerm} = this.props

    const filtered = list.filter(item => {
      const isInList = searchKeys.some(key => {
        if (_.isObject(item[key])) {
          throw new Error(
            `The value at key "${key}" is an object.  Take a look at "searchKeys" and 
             make sure you're indexing onto a primitive value`
          )
        }

        const value = _.get(item, key, '')

        if (value === '') {
          throw new Error(`${key} is undefined.  Take a look at "searchKeys". `)
        }

        return String(value)
          .toLocaleLowerCase()
          .includes(searchTerm.toLocaleLowerCase())
      })

      return isInList
    })

    return filtered
  }
}

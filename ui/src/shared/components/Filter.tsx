// Libraries
import {PureComponent} from 'react'

import {
  get,
  sortBy,
  isString,
  isArray,
  isEmpty,
  isObject,
  flatMap,
  sortedIndex,
} from 'lodash'

// Types
import {Label} from 'src/types'

// searchKeys: the keys whose values you want to filter on
// if the values are nested use dot notation i.e. tasks.org.name

export interface OwnProps<T> {
  list: T[]
  searchTerm: string
  searchKeys: string[]
  sortByKey?: string
  children: (list: T[]) => any
}

export interface StateProps {
  labels: {[uuid: string]: Label}
}

export type Props<T> = OwnProps<T> & StateProps

const INEXACT_PATH = /\w+\[\]/g
const EMPTY_ARRAY_BRACKETS = /\[\]?\./
/**
 * Filters a list using a searchTerm and searchKeys where
 *  searchKeys is an array of strings represents either an
 *  exact or inexact path to a property value(s):
 *  "user.name" (exact) or "authors[].name" (inexact)
 *
 */

export default class FilterList<T> extends PureComponent<Props<T>> {
  public render() {
    return this.props.children(this.sorted)
  }

  private get sorted(): T[] {
    return sortBy<T>(this.filtered, [
      (item: T) => {
        const value = get(item, this.props.sortByKey)

        if (!!value && isString(value)) {
          return value.toLocaleLowerCase()
        }

        return value
      },
    ])
  }

  private get filtered(): T[] {
    const {list, labels, searchKeys} = this.props
    const {formattedSearchTerm} = this

    if (isEmpty(formattedSearchTerm)) {
      return list
    }

    const filtered = list.filter(listItem => {
      const item = {
        ...listItem,
        labels: get(listItem, 'labels', []).map(labelID => labels[labelID]),
      }

      const isInList = searchKeys.some(key => {
        const value = this.getKey(item, key)

        const isStringArray = this.isStringArray(value)

        if (!isStringArray && isObject(value)) {
          throw new Error(
            `The value at key "${key}" is an object.  Take a look at "searchKeys" and
             make sure you're indexing onto a primitive value`
          )
        }

        if (isStringArray) {
          const searchIndex = this.createIndex(value)
          return this.checkIndex(searchIndex, formattedSearchTerm)
        }

        return String(value)
          .toLocaleLowerCase()
          .includes(formattedSearchTerm)
      })

      return isInList
    })

    return filtered
  }

  private isStringArray(value: any): boolean {
    if (!isArray(value)) {
      return false
    }

    if (isEmpty(value) || isString(value[0])) {
      return true
    }

    return false
  }

  private get formattedSearchTerm(): string {
    return this.props.searchTerm.trimLeft().toLocaleLowerCase()
  }

  private getKey(item: T, key: string) {
    const isInexact = key.match(INEXACT_PATH)

    if (!isInexact) {
      return get(item, key, '')
    } else {
      return this.getInExactKey(item, key)
    }
  }

  private getInExactKey(item: T, key: string) {
    const paths = key.split(EMPTY_ARRAY_BRACKETS)
    // flattens nested arrays into one large array
    const values = paths.reduce(
      (results, path) => flatMap(results, r => get(r, path, [])),
      [item]
    )

    return values
  }

  private createIndex = (terms: string[]) => {
    return flatMap(terms, this.extractSuffixes).sort()
  }

  private checkIndex = (sortedSuffixes: string[], searchTerm) => {
    const index = sortedIndex(sortedSuffixes, searchTerm)
    const nearestSuffix = sortedSuffixes[index]

    if (!!nearestSuffix && nearestSuffix.includes(searchTerm)) {
      return true
    }

    return false
  }

  private extractSuffixes = (term: string) => {
    const suffixes = new Array(term.length)
    const lowerTerm = term.toLocaleLowerCase()

    for (let i = 0; i < suffixes.length; i++) {
      suffixes[i] = lowerTerm.slice(i)
    }

    return suffixes
  }
}

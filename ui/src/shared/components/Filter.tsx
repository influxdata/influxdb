import {PureComponent} from 'react'
import _ from 'lodash'

// searchKeys: the keys whose values you want to filter on
// if the values are nested use dot notation i.e. tasks.org.name

interface Props<T> {
  list: T[]
  searchTerm: string
  searchKeys: string[]
  sortByKey?: string
  children: (list: T[]) => any
}

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
    return this.props.children(_.sortBy(this.filtered, this.props.sortByKey))
  }

  private get filtered(): T[] {
    const {list, searchKeys} = this.props
    const {formattedSearchTerm} = this

    if (_.isEmpty(formattedSearchTerm)) {
      return list
    }

    const filtered = list.filter(item => {
      const isInList = searchKeys.some(key => {
        const value = this.getKey(item, key)

        const isStringArray = this.isStringArray(value)

        if (!isStringArray && _.isObject(value)) {
          throw new Error(
            `The value at key "${key}" is an object.  Take a look at "searchKeys" and 
             make sure you're indexing onto a primitive value`
          )
        }

        if (isStringArray) {
          const searchIndex = this.createIndex(value)
          return this.checkIndex(searchIndex, formattedSearchTerm)
        }

        if (value === '') {
          throw new Error(`${key} is undefined.  Take a look at "searchKeys". `)
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
    if (!_.isArray(value)) {
      return false
    }

    if (_.isEmpty(value) || _.isString(value[0])) {
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
      return _.get(item, key, '')
    } else {
      return this.getInExactKey(item, key)
    }
  }

  private getInExactKey(item: T, key: string) {
    const paths = key.split(EMPTY_ARRAY_BRACKETS)
    // flattens nested arrays into one large array
    const values = paths.reduce(
      (results, path) => _.flatMap(results, r => _.get(r, path, [])),
      [item]
    )

    return values
  }

  private createIndex = (terms: string[]) => {
    return _.flatMap(terms, this.extractSuffixes).sort()
  }

  private checkIndex = (sortedSuffixes: string[], searchTerm) => {
    const index = _.sortedIndex(sortedSuffixes, searchTerm)
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

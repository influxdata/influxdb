// Libraries
import {PureComponent} from 'react'
import {orderBy} from 'lodash'

// Types
import {Sort} from '@influxdata/clockface'

interface Props<T> {
  list: T[]
  sortKey?: string
  direction?: Sort
  children: (sortedList: T[]) => JSX.Element
}

export default class SortingHat<T> extends PureComponent<Props<T>> {
  public render() {
    return this.props.children(this.sorted)
  }

  private get sorted(): T[] {
    const {list, sortKey, direction} = this.props

    return orderBy(list, [sortKey], [direction])
  }
}

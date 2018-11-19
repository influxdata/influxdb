// Libraries
import {PureComponent} from 'react'
import {orderBy} from 'lodash'

// Types
import {Sort} from 'src/clockface/types'

interface Props<T> {
  list: T[]
  sortKeys?: string[]
  directions?: Sort[]
  children: (sortedList: T[]) => JSX.Element
}

export default class SortingHat<T> extends PureComponent<Props<T>> {
  public render() {
    return this.props.children(this.sorted)
  }

  private get sorted(): T[] {
    const {list, sortKeys, directions} = this.props

    return orderBy(list, sortKeys, directions)
  }
}

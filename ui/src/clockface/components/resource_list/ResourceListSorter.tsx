// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Types
import {Sort} from 'src/clockface/types'

interface Props {
  sortKey: string
  sort: Sort
  name: string
  onClick?: (nextSort: Sort, sortKey: string) => void
}

export default class ResourceListSorter extends PureComponent<Props> {
  public render() {
    const {name} = this.props

    return (
      <div
        className={this.className}
        onClick={this.handleClick}
        title={this.title}
      >
        {name}
        {this.sortIndicator}
      </div>
    )
  }

  private handleClick = (): void => {
    const {onClick, sort, sortKey} = this.props

    if (!onClick || !sort) {
      return
    }

    if (sort === Sort.None) {
      onClick(Sort.Ascending, sortKey)
    } else if (sort === Sort.Ascending) {
      onClick(Sort.Descending, sortKey)
    } else if (sort === Sort.Descending) {
      onClick(Sort.None, sortKey)
    }
  }

  private get title(): string {
    const {sort, name} = this.props

    if (sort === Sort.None) {
      return `Sort ${name} in ${Sort.Ascending} order`
    } else if (sort === Sort.Ascending) {
      return `Sort ${name} in ${Sort.Descending} order`
    }
  }

  private get sortIndicator(): JSX.Element {
    const {onClick} = this.props

    if (onClick) {
      return (
        <span className="resource-list--sort-arrow">
          <span className="icon caret-down" />
        </span>
      )
    }
  }

  private get className(): string {
    const {sort} = this.props

    return classnames('resource-list--sorter', {
      'resource-list--sort-descending': sort === Sort.Descending,
      'resource-list--sort-ascending': sort === Sort.Ascending,
    })
  }
}
